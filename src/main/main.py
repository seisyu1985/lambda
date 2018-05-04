# coding: UTF-8
import requests
import pymysql.cursors
import json
import os
from base64 import b64decode
import boto3
from datetime import datetime

today = None
kms = boto3.client('kms')

# 次ユーザ取得SQL
next_user_sql = " select us.id as id, us.sk_id as sk_id from users us " \
                "     left join ( select max(users_id) as users_id from requests " \
                "         where request_date = (select max(request_date) from requests) " \
                "     ) req " \
                " on us.id > req.users_id " \
                " where us.is_target=1 " \
                " order by req.users_id is null asc, us.id asc limit 1 "
# 前回ユーザ取得SQL
before_user_sql = " select req.id, req.users_id, date_format(req.request_date, '%Y-%m-%d') as request_date, us.wp_id, us.sk_id from requests req " \
                  " inner join users us on req.users_id = us.id and us.is_target = 1"
# 前回ユーザ削除SQL 
delete_request_user = " delete from requests where id = {0}"
# 前回ユーザ更新テキスト
update_request_user = " update requests set count = count + 1 where id = {0} "
# 今回ユーザ新規SQL
insert_requests_sql = " insert into requests (users_id, count, request_date) values ({0}, 1, str_to_date('{1}', '%Y-%m-%d'))"

# 呼び出し
def main(event, context):
    # 現在の年月日を取得
    today = datetime.now().strftime("%Y-%m-%d")
    # 前回担当者を元に今回担当者を取得
    next_us = next_user()
    # 前回担当者を取得し、SSH経由で確認する
    before_user()
    # 今回担当者処理を行う
    next_user_register(next_us[0], today)

# 今回担当者を取得する
def next_user():
    sql = next_user_sql
    return select_mysql(sql)

# 前回担当者の記事記載をチェックする
def before_user():
    users = select_mysql(before_user_sql)

    # SSH経由で確認
    clientLambda = boto3.client("lambda")
    # 引数
    params = {
        "users": users
    }
    
    res = clientLambda.invoke(
        FunctionName="blog_write_check",
        InvocationType="RequestResponse",
        Payload=json.dumps(params)
    )
    # ResponseをJson形式に変換    
    pay=json.loads(res['Payload'].read())

    # DB更新
    before_users_save_db(pay["wordpress_ret"])
    # Slack送信
    before_uses_slack_send(pay["wordpress_ret"])

def before_users_save_db(users):
    array_sql = []

    for user in users:
        if user["ret"] == True:
            # delete
            array_sql.append(delete_request_user.format(user["id"]))
        else:
            # update
            array_sql.append(update_request_user.format(user["id"]))
    # SQL実行
    save_mysql(array_sql)

def before_uses_slack_send(users):
    for user in users:
        if user['ret'] == True:
            msg = "id:{0} sk_id:{1} ブログを書いて貰い感謝！！".format(user["id"], user["sk_id"])
            send_slack("カイジ", ":kaiji:", "omsb_info", msg)
        else:
            msg = "id:{0} sk_id:{1} 書いてない！！制裁！！".format(user["id"], user["sk_id"])
            send_slack("兵頭会長", ":hyodo:", "omsb_info", msg)

def next_user_register(user, day):
    # DB更新
    array_sql = []
    array_sql.append(insert_requests_sql.format(user["id"], day))
    save_mysql(array_sql)

    # Slack送信
    msg = "id:{0} sk_id:{1} ブログを書く順番がきたぜ、ぬるりと。".format(user["id"], user["sk_id"])
    send_slack("アカギ", ":akagi:", "omsb_info", msg)

# Slack送信
def send_slack(user_name, icon, channel, msg):
    # WEBHOOK URLを複合化
    webhook = kms.decrypt(CiphertextBlob=b64decode(os.environ['WEBHOOK_URL']))['Plaintext']

    requests.post(webhook, data = json.dumps({
        'text': msg, # 投稿するテキスト
        'username': user_name, # 投稿のユーザー名
        'link_names': 1, # メンションを有効にする
        'channel': channel, # チャンネル
        'icon_emoji': icon, # アイコン
    }))

# SELECT実行
def select_mysql(sql):
    # 結果
    res = []
    # 接続
    conn = create_connect()
    # 実行
    with conn.cursor() as cursor:
        cursor.execute(sql)
        # Select結果を取り出す
        rets = cursor.fetchall()
        for record in rets:
            res.append(record)
    conn.commit()
    conn.close()
    # 結果を返す
    return res

def save_mysql(sql_list):
    # 結果
    conn = create_connect()
    # 実行
    with conn.cursor() as cursor:
        for sql in sql_list:
            cursor.execute(sql)
    conn.commit()
    conn.close()

def create_connect():
    # DB接続情報を環境変数から取得（複合化)
    host = kms.decrypt(CiphertextBlob=b64decode(os.environ['MYSQL_HOST']))['Plaintext']
    user = kms.decrypt(CiphertextBlob=b64decode(os.environ['MYSQL_USER']))['Plaintext']
    password = kms.decrypt(CiphertextBlob=b64decode(os.environ['MYSQL_PASS']))['Plaintext']
    db_name = kms.decrypt(CiphertextBlob=b64decode(os.environ['MYSQL_DB']))['Plaintext']

    # 接続
    conn = pymysql.connect(host=host,
                           user=user,
                           password=password,
                           db=db_name,
                           charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor)
    return conn