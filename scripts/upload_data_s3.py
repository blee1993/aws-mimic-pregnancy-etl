#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Import MIMIC-IV dataset into AWS S3

AWS credentials should configured via AWS CLI
"""
import os
import sys
import logging
import argparse
import subprocess
from getpass import getpass
from datetime import datetime

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger('upload_dataset')

def parse_args():
    parser = argparse.ArgumentParser(
        description="""Imports MIMIC-V dataset into AWS S3 bucket.""",
        usage = '%(prog)s [options]. Use "%(prog)s --help" for more.'
    )
    parser.add_argument('-p', '--localpath', dest='localpath', type=str, required=True, help='Local directory to download files')    
    parser.add_argument('-b', '--bucket', dest='bucket', type=str, required=True, help='S3 bucket to store raw data')
    parser.add_argument('-k', '--key', dest='key', type=str, default ="", help='S3 bucket key. Leave empty for root.')
    parser.add_argument('-d', '--download', dest='download', type=lambda x: x.lower() == 'true', default =True, help='Enables local download of files. Defualt is true.')
    return parser.parse_args()

def sync_files_locally(username,password):
    cmd = [
        "wget",
        "-r",   
        "-N",   
        "-c",   
        "-np",  
        "--user", username,  
        "--password", password,  
        "https://physionet.org/files/mimiciv/3.0/"  
    ]

    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        print(f"Error during download: {e}",file=sys.stderr)
    return


def sync_to_s3(localpath,bucket,key):
    upload_s3_path = f"s3://{bucket}/{key}"
    cmd = ["aws","s3","sync",localpath,upload_s3_path]

    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        print(f"Error during upload: {e}",file=sys.stderr)
    return

def main():
    logger.info('{} - Script start'.format(datetime.now()))
    args = parse_args()

    logger.info('Download flag is set to: {}'.format(args.download))
    
    if args.download:
        # Download files locally
        username = input("Enter your Physionet username: ")
        password = getpass("Enter your Physionet password: ")
        logger.info('{0} - Starting local download of files to {1}'.format(datetime.now(),args.localpath))
        sync_files_locally(username,password)
        logger.info('{0} - Local download of files complete {1}'.format(datetime.now(),args.localpath))

    # Upload files to S3
    logger.info('{0} - Starting upload to {1}'.format(datetime.now(),args.bucket))
    sync_to_s3(args.localpath,args.bucket,args.key)
    logger.info('{0} Upload complete to S3.'.format(datetime.now()))
    
    logger.info('{} - Script end'.format(datetime.now()))

if __name__ == '__main__':
    main()