#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: gxs
@license: (C) Copyright 2016-2019, Light2Cloud (Beijing) Web Service Co., LTD
@contact: dingjianfeng@light2cloud.com
@software: AWS-DJF
@file: bos_and_s3_data.py
@ide: PyCharm
@time: 2020/4/28 14:06
@desc：
"""
import os
import pathlib
import logging
import csv
import fnmatch
import hashlib
import base64
import shutil
import zipfile
import time
import datetime

import boto3
from botocore.exceptions import ClientError

from baidubce.bce_client_configuration import BceClientConfiguration
from baidubce.auth.bce_credentials import BceCredentials
from baidubce.services.sts.sts_client import StsClient
from baidubce.exception import BceError
from baidubce.services.bos.bos_client import BosClient
from baidubce.services.bos import storage_class

import threading


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
LOG_FILE = os.path.join(LOG_DIR, 'data_from_bos_to_s3_all.log')
LOG_FILE_ERROR = os.path.join(LOG_DIR, 'data_from_bos_to_s3_warning.log')
LOG_Danger = os.path.join(LOG_DIR, 'data_danger.log')

if not os.path.isdir(LOG_DIR):
    os.makedirs(LOG_DIR)


class DataFromBOSToS3:

    def __init__(
            self, bce_access_key_id=None, bce_secret_access_key=None,
            bce_bos_host=None, bce_sts_host=None, bce_region=None,
            bos_bucket_name=None, bos_storage_class=None,
            verify_data_after_download=True,
            access_key=None, secret_key=None, region=None,
            aws_session_token=None, profile=None, topic_arn=None,
            bucket=None, s3_storage_class=None,
    ):
        self.logger = self._init_logger()
        self.bce_access_key_id = bce_access_key_id
        self.bce_secret_access_key = bce_secret_access_key
        self.bce_bos_host = bce_bos_host
        self.bce_sts_host = bce_sts_host
        self.bce_region = bce_region
        self.bos_bucket_name = bos_bucket_name
        self.bos_storage_class = bos_storage_class
        self.verify_data_after_download = verify_data_after_download  # 下载后验证数据
        self.accessKey = access_key
        self.secretKey = secret_key
        self.aws_session_token = aws_session_token
        self.profile = profile
        self.region = region
        self.topic_arn = topic_arn
        self.bucket = bucket
        self.s3_storage_class = s3_storage_class

    @staticmethod
    def _init_logger():
        _logging = logging.getLogger('l2c.%s' % __name__)
        _logging.setLevel(10)

        """写入日志文件, 大等于20的日志被写入"""
        fh = logging.FileHandler(LOG_FILE, mode='a', encoding='utf8')
        fh.setLevel(20)
        formatter_fh = logging.Formatter('%(levelname)-3s\t %(asctime)s [%(module)s, %(process)d:%(thread)d] '
                                         '[message]: %(message)s',
                                         datefmt="%Y-%m-%d %H:%M:%S")
        fh.setFormatter(formatter_fh)

        """写入日志文件, 大等于30的日志被写入"""
        fh_error = logging.FileHandler(LOG_FILE_ERROR, mode='a', encoding='utf8')
        fh_error.setLevel(30)
        formatter_fh_error = logging.Formatter('%(levelname)-3s\t %(asctime)s [%(module)s, %(process)d:%(thread)d] '
                                               '[message]: %(message)s',
                                               datefmt="%Y-%m-%d %H:%M:%S")
        fh_error.setFormatter(formatter_fh_error)

        """写入日志文件, 大等于50的日志被写入"""
        fh_critical = logging.FileHandler(LOG_Danger, mode='a', encoding='utf8')
        fh_critical.setLevel(50)
        formatter_fh_critical = logging.Formatter('%(levelname)s %(asctime)s [%(module)s, %(process)d:%(thread)d] '
                                                  '[message]: %(message)s',
                                                  datefmt="%Y-%m-%d %H:%M:%S")
        fh_critical.setFormatter(formatter_fh_critical)

        """输出到终端"""
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter_ch = logging.Formatter('%(asctime)s %(name)s: [line:%(lineno)d]  ' 
                                         '%(levelname)s-[message]: %(message)s',
                                         datefmt="%Y-%m-%d %H:%M:%S")
        ch.setFormatter(formatter_ch)

        """向 _logging 添加handler """
        _logging.addHandler(fh)
        _logging.addHandler(fh_error)
        _logging.addHandler(fh_critical)
        _logging.addHandler(ch)
        return _logging

    def _bce_init_connection(self):
        try:
            bce_config = BceClientConfiguration(
                credentials=BceCredentials(
                    access_key_id=self.bce_access_key_id,
                    secret_access_key=self.bce_secret_access_key),
                endpoint=self.bce_bos_host)
            bos_client = BosClient(bce_config)
            return bos_client

        except BceError as e:
            self.logger.error('使用BCE当前凭证，在连接时发生错误 {}'.format(e))
            return []

        except Exception as e:
            self.logger.exception('使用BCE当前凭证，在连接时发生异常错误 {}'.format(e))
            return []

    def _bce_init_connection_sts(self):
        try:
            bce_config = BceClientConfiguration(
                credentials=BceCredentials(
                    access_key_id=self.bce_access_key_id,
                    secret_access_key=self.bce_secret_access_key),
                endpoint=self.bce_sts_host)
            sts_client = StsClient(bce_config)
            access_dict = {}
            duration_seconds = 3600
            access_dict["service"] = "bce:bos"
            access_dict["region"] = "bj"
            access_dict["effect"] = "Allow"
            resource = ["*"]
            access_dict["resource"] = resource
            permission = ["*"]
            access_dict["permission"] = permission

            access_control_dict = {"accessControlList": [access_dict]}
            response = sts_client.get_session_token(acl=access_control_dict, duration_seconds=duration_seconds)

            config = BceClientConfiguration(
                credentials=BceCredentials(str(response.access_key_id), str(response.secret_access_key)),
                endpoint=self.bce_bos_host,
                security_token=response.session_token)
            bos_client = BosClient(config)
            return bos_client

        except BceError as e:
            self.logger.error('使用BCE当前连接令牌，在连接时发生错误 {}'.format(e))
            return []

        except Exception as e:
            self.logger.exception('使用BCE当前连接令牌，在连接时发生异常错误 {}'.format(e))
            return []

    def _aws_init_connection(self, service):
        try:
            s = boto3.Session(
                aws_access_key_id='{}'.format(self.accessKey),
                aws_secret_access_key='{}'.format(self.secretKey),
                region_name='{}'.format(self.region),
            )
            c = s.client('{}'.format(service))
            return c
        except ClientError as e:
            e.response['Error'].update({'operation_name': e.operation_name})

            self.logger.error('使用AWS当前凭证，在连接时发生错误 {}'.format(e.response['Error']))
            return []

        except Exception as e:
            self.logger.exception('使用AWS当前凭证，在连接时发生异常错误 {}'.format(e))
            return []

    def _aws_init_connection_token(self, service):
        try:
            s = boto3.Session(
                aws_access_key_id='{}'.format(self.accessKey),
                aws_secret_access_key='{}'.format(self.secretKey),
                aws_session_token='{}'.format(self.aws_session_token),
                region_name='{}'.format(self.region),
            )
            c = s.client('{}'.format(service))
            return c
        except ClientError as e:
            e.response['Error'].update({'operation_name': e.operation_name})
            self.logger.error('使用AWS当前连接令牌，在连接时发生错误 {}'.format(e.response['Error']))
            return []

        except Exception as e:
            self.logger.exception('使用AWS当前连接令牌，在连接时发生异常错误 {}'.format(e))
            return []

    def _aws_init_profile(self, service):
        """
        A method to initialize an AWS service connection with an AWS profile.
        :param service:
        :return: (object) the AWS connection object.
        """
        try:
            s = boto3.Session(
                profile_name='{}'.format(self.profile)
            )
            c = s.client('{}'.format(service))
            return c
        except ClientError as e:
            e.response['Error'].update({'operation_name': e.operation_name})

            self.logger.error('使用AWS当前配置文件，在连接时发生错误 {}'.format(e.response['Error']))
            return []

        except Exception as e:
            self.logger.exception('使用AWS当前配置文件，在连接时发生异常错误 {}'.format(e))
            return []

    def main_function(self):
        """
        max_keys=1000
        :return:
        """
        if self.bce_access_key_id is not None and self.bce_sts_host is not None:
            bos_client = self._bce_init_connection_sts()
        else:
            bos_client = self._bce_init_connection()

        try:
            # 'd/2eeQ7f', 'd/1442413150028', 'd/1442754128155', 'd/1444316556440', 'd/jieINz', 'd/yayYVv'
            # file_directory_list = [
            #                        'd/sinldo/bbsy/jk7oxTbYvqiq/1', 'd/sinldo/trsrmyy/XOq7eNQbyEJz/2',
            #                        'd/sinldo/yzrmyy/Pu5WmamyMfYj/3', 'd/sinldo/yzrmyy/QCYljhbqaYR3/4']   # 归档
            # file_directory_list = ['d/2eeQ7f', 'c/1442413150028']
            file_directory_list = self.list_bos_csv()
            fixed_directory_list = []
            while len(file_directory_list) >= 1:
                fixed_directory_list.clear()
                """递进式取1000个参数"""
                fixed_directory_list.extend(file_directory_list[0:2])
                del file_directory_list[0:2]

                """存放被遍历目录下所有子文件夹的列表"""
                sub_folder_list = []
                """存放被遍历目录下所有文件的列表"""
                file_list = []
                size_list = []
                for _dir_list in fixed_directory_list:
                    marker = None
                    is_truncated = True
                    while is_truncated:

                        response = bos_client.list_objects(bucket_name=self.bos_bucket_name,
                                                           max_keys=1000,
                                                           prefix=_dir_list,
                                                           marker=marker)
                        for object in response.contents:
                            if object.size == 0 and object.key[-1] == '/':
                                sub_folder_list.append(object.key)
                            else:
                                file_list.append(object.key)
                                size_list.append(object.size)
                        is_truncated = response.is_truncated
                        marker = getattr(response, 'next_marker', None)

                if sub_folder_list:
                    self.makedir_directory_from_bos(fixed_directory_list, sub_folder_list)
                else:
                    self.makedir_directory_from_bos(fixed_directory_list,)
                self.logger.warning(f'从 BOS 存储桶读取文件总数量：{len(file_list)} ')
                self.logger.warning(f'从 BOS 存储桶读取文件总大小：{self._read_bos_file_size(size_list)} GB ')
                if self._read_bos_file_size(size_list) <= str(700):
                    return self.download_file_from_bos(bos_client, file_list, fixed_directory_list)
                    # return threading.Thread(target=self.download_file_from_bos, name=f'bos',
                    #                         args=(bos_client, file_list, fixed_directory_list)).start()
                    # pass
                else:
                    self.logger.warning(f'从 BOS 存储桶读取文件总大小超过 700 GB')

        except BceError as e:
            self.logger.error('从 BOS 存储桶读取文件详情时，发生错误 {}'.format(e))
            return []

    @staticmethod
    def list_bos_csv() -> list:
        result = []
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        for csv_file in os.listdir(BASE_DIR):
            if fnmatch.fnmatch(csv_file, '?_aws_mig_*.csv'):
                with open(csv_file, mode='r', encoding='utf8', newline='') as csv_file:
                    reader = csv.reader(csv_file)
                    for item in reader:
                        if reader.line_num == 1 and item[0] == "concat('d/',site,'/',owner,'/',store_uid,'/')":
                            continue
                        result.append(item[0])
        return result

    def makedir_directory_from_bos(self, directories: list, sub_folders: list = None):
        try:
            if sub_folders:
                for directory in directories:
                    if not os.path.isdir(directory):
                        os.makedirs(directory)
                for sub_folder in sub_folders:
                    if not os.path.isdir(sub_folder):
                        os.makedirs(sub_folder)
            else:
                for directory in directories:
                    if not os.path.isdir(directory):
                        os.makedirs(directory)
        except FileExistsError as e:
            self.logger.error('创建对应的多级目录时，发生错误 {}'.format(e))

    @staticmethod
    def _read_bos_file_size(size_list: list) -> str:
        size_sum = 0
        for n in size_list:
            size_sum = size_sum + n
        size_sum_gb = size_sum / (1024 * 1024 * 1024)
        size_sum_gb = ("%.5f" % size_sum_gb)
        return size_sum_gb

    def __upload_file_to_bos(self, file_lists: list):
        """
        BOS储存类型: # 标准存储	STANDARD | 低频存储 STANDARD_IA | 冷存储 COLD | 归档存储	ARCHIVE

        :param file_lists:
        :return:
        """
        if self.bce_access_key_id is not None and self.bce_sts_host is not None:
            bos_client = self._bce_init_connection_sts()
        else:
            bos_client = self._bce_init_connection()

        try:
            for file in file_lists:
                response = bos_client.put_object_from_file(
                    bucket=self.bos_bucket_name,
                    file_name=file,
                    key=os.path.basename(file),
                    storage_class=self.bos_storage_class,
                )
                self.logger.info(f'文件：{file} 上传到 BOS 存储桶成功')

        except BceError as e:
            self.logger.error('本地数据上传到 BOS 存储桶时，发生错误 {}'.format(e))
            return []

    def download_file_from_bos(self, bos_client, file_lists: list, file_directory_list: list):
        """
        :param bos_client:
        :param file_lists: list BOS 数据列表
        :param file_directory_list: CSV 路径列表
        :return:
        """
        try:
            for file in file_lists:
                path = pathlib.Path(file)
                if path.is_file():
                    pass
                    # self.logger.info(f'BOS 存储桶中的文件：{file} 在本地存在，不执行下载操作')
                else:
                    if not os.path.isdir(os.path.dirname(file)):
                        os.makedirs(os.path.dirname(file))
                    if bos_client.get_object_meta_data(bucket_name=self.bos_bucket_name,
                                                       key=file).metadata.bce_storage_class == 'ARCHIVE':
                        self.logger.critical(f'BOS 归档文件：{file} ')
                        continue
                    response = bos_client.get_object_to_file(
                        bucket_name=self.bos_bucket_name,
                        key=file,
                        file_name=file,
                    )
                    # self.logger.info(f'BOS 存储桶中的文件：{file} 下载到本地')

                    content_md5 = response.metadata.content_md5
                    self.check_file_md5(bos_client=bos_client, file_name=file, file_content_md5=content_md5,
                                        file_directory_list=file_directory_list)

        except BceError as e:
            self.logger.error(f'从 BOS 存储桶下载文件 时，发生错误 {e}')
            return []

        except Exception as e:
            self.logger.exception(f'从 BOS 存储桶下载文件时，发生错误 {e} ')
            return []

        finally:
            for base_path in file_directory_list:
                self._delete_old_zip(base_path)
                self.zip_file(base_path)

    def check_file_md5(self, bos_client, file_name: str, file_content_md5: str, file_directory_list: list):
        """
        :param bos_client:
        :param file_name:
        :param file_content_md5:
        :param file_directory_list:
        :return:
        """
        md5 = self._count_md5(file_name)
        if file_content_md5 == md5[0]:
            self.logger.info(f'下载、校验文件：{file_name} 完成，数据完整，content_md5：{file_content_md5} ')
        else:
            self.logger.warning(f'下载校验文件：{file_name} 发现数据损坏..... 原始 content_md5：{file_content_md5} '
                                f'下载后 content_md5：{md5[0]} ')
            return self._download_check_corrupt_file(bos_client, file_name, file_directory_list)

    @staticmethod
    def _count_md5(file_name):
        buf_size = 8192
        with open(file_name, 'rb') as fp:
            file_md5 = hashlib.md5()
            while True:
                bytes_to_read = buf_size
                buf = fp.read(bytes_to_read)
                if not buf:
                    break
                file_md5.update(buf)
            etag = file_md5.hexdigest()
        content_md5 = str(base64.standard_b64encode(file_md5.digest()), encoding='utf-8')
        return [content_md5, etag]

    def _download_check_corrupt_file(self, bos_client, file: str, file_directory_list: list):
        if file:
            self._delete_file(file)
        self.logger.info(f'删除下载的受损文件：{file} ')
        try:
            response = bos_client.get_object_to_file(
                bucket_name=self.bos_bucket_name,
                key=file,
                file_name=file,
            )
            self.logger.info(f'下载到本地的文件：{file} 受损，重新下载至本地')

            file_content_md5 = response.metadata.content_md5
            file_etag = response.metadata.etag
            md5 = self._count_md5(file)

            if file_content_md5 == md5[0] and file_etag == md5[1]:
                self.logger.warning(f'重新下载、校验的文件：{file} 完成，'
                                    f'经过对比 content_md5：{file_content_md5} 和 etag：{file_etag} '
                                    f'数据完整')
            else:
                self.logger.critical(f'重新下载校验文件：{file} 发现数据仍然有问题..... '
                                     f'原始 content_md5：{file_content_md5} '
                                     f'下载后 content_md5：{md5[0]} ')

                return self._choose_corrupt_write_to_csv(str(file), file_directory_list)

        except BceError as e:
            self.logger.error('从 BOS 存储桶再次下载源受损文件时，发生错误 {}'.format(e))
            return []

    def _delete_file(self, file: str):
        try:
            corrupt_file = pathlib.Path(file)
            os.remove(corrupt_file.absolute())
        except OSError as e:
            self.logger.error(f'删除本地受损文件：{file} 发生错误 {e.strerror}')

    @staticmethod
    def _read_csv_data(csv_file: str):
        if not os.path.isfile(csv_file):
            with open(csv_file, mode='a', encoding='utf8'):
                pass
        else:
            csv_data_list = []
            with open(csv_file, mode='r', encoding='utf8') as f:
                csv_read = csv.reader(f)
                for line in csv_read:
                    if line:
                        csv_data_list.extend(line)
            return csv_data_list

    def _choose_corrupt_write_to_csv(self, file: str, file_directory_list: list):
        file_csv = 'failed_data_check_after_second_download.csv'
        csv_file_list = self._read_csv_data(str(file_csv))
        with open(file=file_csv, mode='a', encoding='utf8') as f:
            if not csv_file_list:
                csv_write = csv.writer(f)
                csv_write.writerow([file])
                self.logger.warning(f'将二次下载校验后出现误差的数据：{file} 写入csv文件中')
            else:
                if file not in set(csv_file_list):
                    csv_write = csv.writer(f)
                    csv_write.writerow([file])
                    self.logger.warning(f'将二次下载校验后出现误差的数据：{file} 写入csv文件中')

        base_path_csv = 'failed_data_dir_path_check_after_second_download.csv'
        csv_base_path_list = self._read_csv_data(str(base_path_csv))
        with open(file=str(base_path_csv), mode='a', encoding='utf8') as f:
            for base_path in file_directory_list:
                if file.startswith(base_path):
                    if not csv_base_path_list:
                        csv_write = csv.writer(f)
                        csv_write.writerow([base_path])
                        self.logger.critical(f'将下载后出现误差数据的路径：{base_path} 写入csv文件中')
                    else:
                        if base_path not in set(csv_base_path_list):
                            csv_write = csv.writer(f)
                            csv_write.writerow([base_path])
                            self.logger.critical(f'将下载后出现误差数据的路径：{base_path} 写入csv文件中')

    @staticmethod
    def __get_time(day=0, hour=0, minute=0, second=0, get_time_type=None):
        bj_now_time = datetime.datetime.now().replace(tzinfo=None)
        if get_time_type == "time_stamp":
            now_tm = (bj_now_time + datetime.timedelta(
                days=day, hours=hour, minutes=minute, seconds=second
            )).strftime("%Y-%m-%d %H:%M:%S")
            time_array = time.strptime(now_tm, "%Y-%m-%d %H:%M:%S")
            return_tm = str(int(time.mktime(time_array)))
        elif get_time_type == "time_stamp_sf":
            return_tm = (bj_now_time + datetime.timedelta(
                days=day, hours=hour, minutes=minute, seconds=second)).strftime("__%Y%m%d%H%M%S")
        elif get_time_type == "time_stamp_sp":
            return_tm_sf = (bj_now_time + datetime.timedelta(
                days=day, hours=hour, minutes=minute, seconds=second)).strftime("__%Y%m%d%H%M%S")
            return_tm = datetime.datetime.strptime(return_tm_sf, "__%Y%m%d%H%M%S")
        else:
            return_tm = bj_now_time
        return return_tm

    def _delete_old_zip(self, directory: str):
        try:
            for f_name in os.listdir(directory):
                if fnmatch.fnmatch(f_name, '__*.zip'):
                    os.remove(pathlib.Path(directory, f_name))
                    self.logger.info(f'删除旧压缩包：{f_name}')
        except OSError as e:
            self.logger.error(f'删除旧压缩包发生错误 {e.strerror}')

    def zip_file(self, base_path: str):
        file_lists = self._get_correct_data_to_zip(base_path)
        try:
            # zip_name = self.__get_time(get_time_type="time_stamp_sf")
            # dir_zip_name = pathlib.Path(base_path, "__" +
            #                             datetime.datetime.now().replace(tzinfo=None).strftime("%Y%m%d-") +
            #                             pathlib.Path(base_path).name + ".zip")
            dir_zip_name = os.path.join(base_path, "__" + pathlib.Path(base_path).name + ".zip").replace('\\', '/')
            with zipfile.ZipFile(
                    dir_zip_name,
                    'w') as new_zip:
                for entry in file_lists:
                    file_path = os.path.join(base_path, entry[1:])
                    new_zip.write(file_path, entry)
            if len(new_zip.namelist()) == 0:
                self.logger.critical(f'压缩当前路径：{base_path} 压缩数量：0，不上传该压缩包')
            else:
                self.logger.warning(f'压缩当前路径：{base_path} 压缩数量：{len(new_zip.namelist())} 压缩文件列表：{new_zip.namelist()} ')
                return self.upload_file_to_s3(dir_zip_name)

        except OSError as e:
            self.logger.error(f'压缩本地文件时发生错误 {e.strerror}')

    @staticmethod
    def _get_correct_data_to_zip(base_path: str):
        file_dir_lists = []
        for dir_path, dir_names, files in os.walk(base_path):
            for file in files:
                file_dir_lists.append(os.path.join(dir_path, file))
        file_lists = []
        for file in file_dir_lists:
            if file.startswith(base_path):
                file_lists.append(file[len(base_path):])
        return file_lists

    def _read_zip_md5(self, dir_zip_name: str):
        md5 = self._count_md5(dir_zip_name)
        # self.logger.info(f'计算压缩包：{dir_zip_name} 的MD5值，'
        #                  f'content_md5：{md5[0]}  etag：{md5[1]} ')
        return md5

    def upload_file_to_s3(self, zip_file: str):
        """
        储存类型：'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'|'DEEP_ARCHIVE',
        :param zip_file:
        :return:
        """
        if self.accessKey is not None and self.aws_session_token is not None:
            s3c = self._aws_init_connection_token('s3')
        elif self.accessKey is not None:
            s3c = self._aws_init_connection('s3')
        elif self.profile is not None:
            s3c = self._aws_init_profile('s3')
        else:
            s3c = boto3.client('s3', region_name=self.region)

        try:
            zip_file_dir = os.path.abspath(zip_file)

            with open(zip_file_dir, 'rb') as fd:
                response = s3c.put_object(
                        Body=fd,
                        Bucket=self.bucket,
                        Key=zip_file,
                        StorageClass=self.s3_storage_class,
                    )

            etag_zip_md5 = self._read_zip_md5(zip_file)[1]
            new_etag = response['ETag'].replace('"', "")

            if new_etag == etag_zip_md5:
                self.logger.info(f'上传、校验压缩包：{zip_file} 完成，数据完整，ETag：{new_etag} ')
                return self.delete_uploaded_zip_of_path(zip_file)
            else:

                self.logger.warning(f"上传校验压缩包：{zip_file} 发现上传中数据损坏..... 原始 ETag：{etag_zip_md5} "
                                    f"上传后 ETag：{new_etag} ")
                self._upload_check_corrupt_file(s3_client=s3c, zip_dir=zip_file_dir, file=zip_file)

        except ClientError as e:
            e.response['Error'].update({'operation_name': e.operation_name})
            self.logger.error('上传压缩包到S3存储桶时，发生错误 {}'.format(e.response['Error']))
            return []

    def delete_uploaded_zip_of_path(self, zip_file: str):
        file_dir = pathlib.Path(zip_file).parent
        try:
            shutil.rmtree(file_dir)
            p = pathlib.Path(file_dir).parent
            if not os.listdir(p):
                p.rmdir()
            self.logger.info(f'压缩包：{zip_file} 上传结束，删除对应路径: {file_dir} 下的所有文件 ')
        except OSError as e:
            self.logger.error(f'压缩包上传结束，删除对应路径: {file_dir} 的所有文件: 发生错误：{e.strerror}')

    def _upload_check_corrupt_file(self, s3_client, zip_dir: str, file: str):
        try:
            with open(zip_dir, 'rb') as fd:
                response = s3_client.put_object(
                    Body=fd,
                    Bucket=self.bucket,
                    Key=file,
                    StorageClass=self.s3_storage_class,
                )
            etag_zip_md5 = self._read_zip_md5(file)[1]
            new_etag = response['ETag'].replace('"', "")
            if new_etag == etag_zip_md5:
                self.logger.warning(f'重新、上传校验压缩包：{file} 完成，数据完整，ETag：{new_etag} ')
            else:

                self.logger.critical(f"重新上传校验压缩包：{file} 发现上传中数据损坏..... "
                                     f"原始 ETag：{etag_zip_md5} "
                                     f"上传后 ETag：{new_etag} ")

                return self._choose_corrupt_zip_write_to_csv(str(file))

        except ClientError as e:
            e.response['Error'].update({'operation_name': e.operation_name})
            self.logger.error('重新上传压缩包到S3存储桶时，发生错误 {}'.format(e.response['Error']))
            return []

    def _choose_corrupt_zip_write_to_csv(self, file: str):
        file_csv = 'second_upload_check_failed_data.csv'
        csv_file_list = self._read_csv_data(str(file_csv))
        with open(file=file_csv, mode='a', encoding='utf8') as f:
            if not csv_file_list:
                csv_write = csv.writer(f)
                csv_write.writerow([file])
                self.logger.warning(f'将二次上传校验后出现误差的数据：{file} 写入csv文件中')
            else:
                if file not in set(csv_file_list):
                    csv_write = csv.writer(f)
                    csv_write.writerow([file])
                    self.logger.warning(f'将二次上传校验后出现误差的数据：{file} 写入csv文件中')

        base_path_csv = 'second_upload_check_failed_data_dir_path.csv'
        csv_base_path_list = self._read_csv_data(str(base_path_csv))
        file_dir = str(pathlib.Path(file).parent)
        with open(file=str(base_path_csv), mode='a', encoding='utf8') as f:
            if not csv_base_path_list:
                csv_write = csv.writer(f)
                csv_write.writerow([file_dir])
                self.logger.critical(f'将上传后出现误差数据的路径：{file_dir} 写入csv文件中')
            else:
                if file_dir not in set(csv_base_path_list):
                    csv_write = csv.writer(f)
                    csv_write.writerow([file_dir])
                    self.logger.critical(f'将上传后出现误差数据的路径：{file_dir} 写入csv文件中')


if __name__ == '__main__':
    print("root_dir:         ", BASE_DIR)
    print("log_file:         ", LOG_FILE)
    print("log_file_warning: ", LOG_FILE_ERROR)
    print("log_file_danger:  ", LOG_Danger)

    bos_s3 = DataFromBOSToS3(
        # BOS
        bce_access_key_id='',
        bce_secret_access_key='',
        bos_bucket_name='',
        bce_bos_host='https://bj.bcebos.com',
        # bce_sts_host='http://sts.bj.baidubce.com',
        # bos_storage_class=storage_class.STANDARD,

        # s3
        access_key='',
        secret_key='',
        region='',  # cn-northwest-1
        bucket='',
        s3_storage_class='',  # STANDARD
    )
    bos_s3.main_function()
