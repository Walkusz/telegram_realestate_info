"""
real estate analysis msg to telegram chat

Usage: main
"""
import boto3
import pandas as pd
import os
import requests
from docopt import docopt
from celery import Celery

app = Celery('tasks', broker='pyamqp://guest@localhost//')


AWS_ACCESS_KEY_ID = os.environ.get('AWSAccessKeyId')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWSSecretKey')
BOT_TOKEN = os.environ.get('BOT_TOKEN')
BOT_CHAT_ID = os.environ.get('BOT_CHAT_ID')
BASE_STRING = 'https://api.telegram.org/bot' + BOT_TOKEN

TABLE_NAME = 'rynek_pierwotny'
REGION_NAME = 'eu-north-1'


@app.task
def telegram_bot_sendtext(bot_message):
    send_text = BASE_STRING + '/sendMessage?chat_id=' + BOT_CHAT_ID + '&parse_mode=html&text=' + bot_message
    response = requests.get(send_text)
    return response.json()


def main():
    dynamodb = boto3.resource('dynamodb',
                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                              region_name=REGION_NAME)
    table = dynamodb.Table(TABLE_NAME)

    table_data = table.scan().get('Items', [])
    df = pd.DataFrame(table_data)
    date_to_use = df['parse_date'].max()
    df.sort_values(by='parse_date', ascending=False, inplace=True)
    df = df[df['parse_date'] == date_to_use]
    df['price'] = pd.to_numeric(df['price'], downcast='float')
    df = df.groupby(by='city')['price'].mean().to_frame()
    table_markdown = df.reset_index().to_markdown(index=False)
    date_text = f'\nData as of: {date_to_use}'
    telegram_bot_sendtext(table_markdown+date_text)


if __name__ == '__main__':
    arguments = docopt(__doc__, version='real estate analysis v0')
    main()
