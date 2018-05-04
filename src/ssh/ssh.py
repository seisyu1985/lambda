# -*- coding: utf-8 -*-
from sshtunnel import SSHTunnelForwarder
# モジュール読み込み
import pymysql.cursors
import os
from base64 import b64decode
import boto3

kms = boto3.client('kms')

def is_blog_write(event, context):

    ssh_hostname = kms.decrypt(CiphertextBlob=b64decode(os.environ['SSH_HOST_NAME']))['Plaintext']
    ssh_port_no = int(kms.decrypt(CiphertextBlob=b64decode(os.environ['SSH_PORT_NO']))['Plaintext'])
    ssh_username = kms.decrypt(CiphertextBlob=b64decode(os.environ['SSH_USER_NAME']))['Plaintext']
    ssh_user_password = kms.decrypt(CiphertextBlob=b64decode(os.environ['SSH_USER_PASSWORD']))['Plaintext']

    # SSH関連の設定
    with SSHTunnelForwarder(
    (
        ssh_hostname, ssh_port_no),
        ssh_host_key=None,
        ssh_username=ssh_username,
        ssh_password=ssh_user_password,
        ssh_pkey=None,
        remote_bind_address=("127.0.0.1", 3306)
    ) as server:

        mysql_username = kms.decrypt(CiphertextBlob=b64decode(os.environ['MYSQL_USER_NAME']))['Plaintext']
        mysql_user_password = kms.decrypt(CiphertextBlob=b64decode(os.environ['MYSQL_USER_PASSWORD']))['Plaintext']
        mysql_schema_name = kms.decrypt(CiphertextBlob=b64decode(os.environ['MYSQL_SCHEMA_NAME']))['Plaintext']

        # MySQLに接続する
        connection = pymysql.connect(host='127.0.0.1',
                                    user=mysql_username,
                                    password=mysql_user_password,
                                    db=mysql_schema_name,
                                    charset='utf8',
                                    port=server.local_bind_port,
                                    # Select結果をtupleではなくdictionaryで受け取れる
                                    cursorclass=pymysql.cursors.DictCursor)
        # 結果
        array = []
        # SQLを実行する
        for k in event['users']:
            with connection.cursor() as cursor:
                sql = "select count(id) as count " \
                        " from " \
                        " wp_posts " \
                        " where " \
                        " post_author = %s " \
                        " and " \
                        " post_date > %s " \
                        " and " \
                        " post_status = 'publish' " \
                        " and " \
                        " post_type = 'post' "
                cursor.execute(sql, (k["wp_id"], k["request_date"]))

                # Select結果を取り出す
                rets = cursor.fetchall()
                # 結果
                for r in rets:
                    if r["count"] >= 1:
                        array.append({"id":k["id"], "users_id":k["users_id"], "sk_id":k["sk_id"], "ret":True})
                    else:
                        array.append({"id":k["id"], "users_id":k["users_id"], "sk_id":k["sk_id"], "ret":False})
                # cursorクローズ
            cursor.close
        # MySQLから切断する
        connection.close()

    return {"wordpress_ret":  array}
