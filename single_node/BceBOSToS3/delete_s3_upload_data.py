#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: gxs
@license: (C) Copyright 2016-2019, Light2Cloud (Beijing) Web Service Co., LTD
@contact: dingjianfeng@light2cloud.com
@software: AWS-DJF
@file: delete_s3_upload_data.py
@ide: PyCharm
@time: 2020/4/16 11:18
@desc：
"""
import base64
import csv
import fnmatch
import hashlib
import os
import pathlib
import shutil

import boto3
import logging
from botocore.exceptions import ClientError

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, 'delete_logs')
LOG_FILE = os.path.join(LOG_DIR, 'upload_to_s3_all.log')
LOG_FILE_ERROR = os.path.join(LOG_DIR, 'upload_to_s3_warning.log')
LOG_Danger = os.path.join(LOG_DIR, 'upload_to_s3_data_danger.log')

if not os.path.isdir(LOG_DIR):
    os.makedirs(LOG_DIR)


class DeleteUploadFile:

    def __init__(
            self, access_key=None, secret_key=None, region=None,
            aws_session_token=None, profile=None, topic_arn=None,
            bucket=None,
    ):
        self.logger = self._init_logger()
        self.accessKey = access_key
        self.secretKey = secret_key
        self.aws_session_token = aws_session_token
        self.profile = profile
        self.region = region
        self.topic_arn = topic_arn
        self.bucket = bucket

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

    def find_zip_file(self):
        # file_directory_list = ['d/2eeQ7f/', 'd/1442413150028/', 'd/1442754128155/', 'd/1444316556440/',
        #                        'd/jieINz/', 'd/yayYVv/']

        file_directory_list = self.list_bos_csv()
        print(file_directory_list)
        zip_file_lists = []
        for base_path in file_directory_list:
            for f_name in os.listdir(base_path):
                if fnmatch.fnmatch(f_name, '__*.zip'):
                    zip_file_lists.append(os.path.join(base_path, f_name))
        return self.delete_file(zip_file_lists)

    @staticmethod
    def list_bos_csv() -> list:
        result = []
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        for csv_file in os.listdir(BASE_DIR):
            if fnmatch.fnmatch(csv_file, '?_aws_mig_*.csv'):
                print(csv_file)
                with open(csv_file, mode='r', encoding='utf8', newline='') as csv_file:
                    reader = csv.reader(csv_file)
                    for item in reader:
                        if reader.line_num == 1 and item[0] == "concat('d/',site,'/',owner,'/',store_uid,'/')":
                            continue
                        result.append(item[0])

        return result

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

    def _read_zip_md5(self, dir_zip_name: str):
        md5 = self._count_md5(dir_zip_name)
        return md5

    def delete_file(self, zip_file_lists: list):
        """
        :param zip_file_lists:
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
            for file in zip_file_lists:
                response = s3c.head_object(
                        Bucket=self.bucket,
                        Key=file,
                    )
                etag_zip_md5 = self._read_zip_md5(file)[1]
                if response['ETag'].replace('"', "") == 1:
                    self.logger.info(f'校验已经上传的压缩包：{file} 完成，数据完整')
                    self.delete_uploaded_zip_of_path(file)
                else:
                    new_etag = response['ETag'].replace('"', "")
                    self.logger.warning(f"校验已经上传的压缩包：{file} 发现上传中数据损坏..... 原始 ETag：{etag_zip_md5} "
                                        f"上传后 ETag：{new_etag} ")
                    self._choose_corrupt_zip_write_to_csv(str(file))

        except ClientError as e:
            e.response['Error'].update({'operation_name': e.operation_name})
            self.logger.error('读取S3存储桶中的数据时，发生错误 {}'.format(e.response['Error']))
            return []

    def delete_uploaded_zip_of_path(self, zip_file: str):
        file_dir = pathlib.Path(zip_file).parent
        try:
            shutil.rmtree(file_dir)
            p = pathlib.Path(file_dir).parent
            if not os.listdir(p):
                p.rmdir()
            return self.logger.info(f'压缩包：{zip_file} 上传结束，删除对应路径: {file_dir} 下的所有文件 ')
        except OSError as e:
            self.logger.error(f'压缩包上传结束，删除对应路径: {file_dir} 的所有文件: 发生错误：{e.strerror}')

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

    def _choose_corrupt_zip_write_to_csv(self, file: str):
        file_csv = 'delete_upload_check_failed_data.csv'
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

        base_path_csv = 'delete_upload_check_failed_data_dir_path.csv'
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

    s3 = DeleteUploadFile(
        # s3
        access_key='',
        secret_key='',
        region='',
        bucket='',
    )
    s3.find_zip_file()
