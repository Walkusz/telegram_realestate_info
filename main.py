"""
real estate analysis msg to telegram chat

Usage: main
"""
import boto3
import pandas as pd
import os
import requests
from docopt import docopt

AWS_ACCESS_KEY_ID = os.environ.get('AWSAccessKeyId')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWSSecretKey')
BOT_TOKEN = os.environ.get('BOT_TOKEN')
BOT_CHAT_ID = os.environ.get('BOT_CHAT_ID')
BASE_STRING = 'https://api.telegram.org/bot' + BOT_TOKEN

TABLE_NAME = 'rynek_pierwotny'
REGION_NAME = 'eu-north-1'


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
    df['price'] = pd.to_numeric(df['price'], downcast='float')
    df = df.groupby(by=['city', 'parse_date'])['price'].mean().to_frame()
    telegram_bot_sendtext(df.reset_index().to_markdown(index=False))


if __name__ == '__main__':
    arguments = docopt(__doc__, version='real estate analysis v0')
    main()
