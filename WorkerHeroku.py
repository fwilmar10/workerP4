import boto
import boto.s3.connection
import boto.rds
#import psycopg2
import subprocess, os
from boto.s3.key import Key
import smtplib
import boto.sqs
from boto.sqs.message import Message
from boto.sqs.connection import SQSConnection
import os



### INICIA PROCESO ##

while True:
        print " - Esperando Mensaje....."
        print " - Esperando Mensaje....."
      