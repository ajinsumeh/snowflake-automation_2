import os
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

smtp_server = os.environ['SMTP_SERVER']
smtp_port = os.environ['SMTP_PORT']
smtp_user = os.environ['SMTP_USER']
smtp_password = os.environ['SMTP_PASSWORD']
approval_email = os.environ['APPROVAL_EMAIL']
file_path = sys.argv[1]

with open(file_path, 'r') as file:
    sql_queries = file.read()

msg = MIMEMultipart()
msg['From'] = smtp_user
msg['To'] = approval_email
msg['Subject'] = 'Approval Request for SQL Execution'

body = f"""
<p>A new SQL file has been uploaded and is pending approval for execution.</p>
<p><strong>File:</strong> {file_path}</p>
<pre>{sql_queries}</pre>
<p><a href="http://your-webhook-service/approve?file_path={file_path}">Approve</a></p>
"""
msg.attach(MIMEText(body, 'html'))

with smtplib.SMTP(smtp_server, smtp_port) as server:
    server.starttls()
    server.login(smtp_user, smtp_password)
    server.send_message(msg)

print(f"Email sent to {approval_email}")
