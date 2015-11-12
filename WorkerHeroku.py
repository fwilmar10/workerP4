import boto
import boto.s3.connection
import boto.rds
import psycopg2
import subprocess, os
from boto.s3.key import Key
import smtplib
import boto.sqs
from boto.sqs.message import Message
from boto.sqs.connection import SQSConnection
import os
from boto3.session import Session
from boto3.dynamodb.conditions import Attr


####### S3 ############################
access_key = os.environ['S3_ACCESS_KEY']
secret_key = os.environ['S3_SECRET_KEY']
conn_s3 = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key)
bucket = conn_s3.get_bucket('s3-appconcurso')

####### SES #####################

fromaddr = "cloudaowf@gmail.com"
toaddrs  = "am.osorio@uniandes.edu.co"
subject = "Concurso de Videos"
text ="""
Estimado usuario,

Su video ha sido procesado y ha sido publicado en la pagina.

Gracias por participar en el concurso

"""
msg = 'Subject: %s\n\n%s' % (subject, text)
smtp_username = os.environ['SMTP_USERNAME']
smtp_password = os.environ['SMTP_PASSWORD']
smtp_port = 465
smtp_server = "email-smtp.us-west-2.amazonaws.com"

#### RDS #######################

#conn = psycopg2.connect("dbname='cloudDB' user='fwilmar10' host='dbinstance.ckrzn0cnbyxh.us-west-2.rds.amazonaws.com' password='Uniandes2013'")
#cur = conn.cursor()

#### DYNAMODB #######################
session = Session(aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key,
                  region_name='us-west-2')

dynamodb = session.resource('dynamodb')

### SQS ########################


access_key_sqs = os.environ['SQS_ACCESS_KEY']
secret_key_sqs = os.environ['SQS_SECRET_KEY']

conn_sqs = boto.sqs.connect_to_region(
       "us-west-2",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)

q=conn_sqs.get_queue('ConvertVideo')


### INICIA PROCESO ##

while True:
        print " - Esperando Mensaje....."
        print " - Esperando Mensaje....."
        rcv_message = q.read()
        if rcv_message != None:
		print " -- Mensaje en Cola"
		print rcv_message.get_body()        
	#video_url = q.get_messages(message_attributes=['VideoURL'])
	#if len(video_url) >= 1:
	#	param1=video_url[0].message_attributes['VideoURL']['string_value']
	#	print ' -- Mensaje en Cola'
        #        print param1
		print '---- INCIO PROCESAMIENTO ----'
	#	pathOriginal = str(param1)
		pathOriginal = str(rcv_message.get_body())
		nombreVideo = pathOriginal.split('/')[-1]
		print '-- Descargando video .... '+nombreVideo
		video= bucket.get_key(pathOriginal)
		print '-- Almacenando video .... '+nombreVideo
		video.get_contents_to_filename('/tmp/orig/'+nombreVideo)
		
		pathConvertido = ''.join(pathOriginal.split('.')[:-1])
		pathConvertido = '{}.mp4'.format(pathConvertido)
		pathConvertido= pathConvertido.replace("/originales/", "/convertidos/")
		print '-- Convirtiendo ....  '+nombreVideo
		name = ''.join(nombreVideo.split('.')[:-1])
		VideoOut = '{}.mp4'.format(name)

		## ACTUALIZAR ESTADO A Procesando  ##
		#cur.execute("""UPDATE concursovideo_video  SET estado = 'Procesando' WHERE video_original='"""+pathOriginal+"'")
		#cur.execute("""commit""")

		tabla = dynamodb.Table('Video')
		response= tabla.scan(
			FilterExpression=Attr('video_original').eq(pathOriginal)
		)

		for item in response['Items']:
			item['estado'] = 'Procesando'
			tabla.put_item(Item=item)

		subprocess.call(['ffmpeg', '-i', '/tmp/orig/'+nombreVideo, '-vcodec', 'libx264', '-crf', '23', '/tmp/convert/'+VideoOut])	
		print '-- Conversion finalizada --'+VideoOut
		nk =Key(bucket, pathConvertido)
		nk.set_contents_from_filename('/tmp/convert/'+VideoOut)	

		## ACTUALIZANDO ESTADO A Convertido ##
		#cur.execute("""UPDATE concursovideo_video  SET estado = 'Convertido',  video_convertido='"""+pathConvertido+"""'  WHERE video_original='"""+pathOriginal+"'")
	       #cur.execute("""commit""")

		tabla = dynamodb.Table('Video')
		response= tabla.scan(
			FilterExpression=Attr('video_original').eq(pathOriginal)
		)

		for item in response['Items']:
			item['estado'] = 'Convertido'
			item['video_convertido'] = pathConvertido
			tabla.put_item(Item=item)

		## ENVIAR CORREO ##
		#print 'SE ENVIA CORREO'
		s = smtplib.SMTP_SSL(smtp_server, smtp_port)
		s.login(smtp_username, smtp_password)
		s.sendmail(fromaddr, toaddrs, msg)

		## BORRAR MENSAJE ##
		#q.delete_message(video_url[0])
		q.delete_message(rcv_message)

		## ELIMINAR ARCHIVOS ##
		if os.path.isfile('/tmp/convert/'+VideoOut):
			os.remove('/tmp/convert/'+VideoOut)
		else:
			print ("Error: %s file not found" % '/tmp/convert/'+VideoOut)		
                if os.path.isfile('/tmp/orig/'+nombreVideo):
                        os.remove('/tmp/orig/'+nombreVideo)
                else:
                        print ("Error: %s file not found" % '/tmp/orig/'+nombreVideo)
	

	







