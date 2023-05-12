import paho.mqtt.client as mqtt
import time
import mysql.connector
import asyncio
import paho.mqtt.client as mqtt


async def run_every_10_seconds():
    while True:
        # 실행할 함수
        db_get = mysql_search()
        print(db_get)
        mqtt_conect(db_get)
        await asyncio.sleep(10)  # 10초 대기
        

def mqtt_conect(db_get):
    # MQTT 브로커에 연결
    global client
    if not client.is_connected():
        client.connect("3.34.2.102", 1883, 60)

    # 메시지 publish
    client.publish("callist", str(db_get))

    # 연결 종료 
    #client.disconnect()


        
# MySQL DB 연결
def mysql_search():
    # MySQL 데이터베이스에 연결
    cnx = mysql.connector.connect(user='test', password='test',
                                   host='15.164.226.13',
                                   database='callist')

    # 쿼리 실행
    query = "SELECT * from User WHERE status is NULL;"

    cursor = cnx.cursor()
    cursor.execute(query)

    # 결과 출력
    for result in cursor:
       return result
    

    # 연결 종료
    cursor.close()
    cnx.close()
# MySQL DB 연결

def mysql_update():
    # MySQL 데이터베이스에 연결
    cnx = mysql.connector.connect(user='test', password='test',
                                   host='15.164.226.13',
                                   database='callist')

    # 쿼리 실행
    query = "SELECT * from User WHERE status is NULL;"

    cursor = cnx.cursor()
    cursor.execute(query)

    # 결과 출력
    for result in cursor:
        print(result)
    return cursor

    # 연결 종료
    cursor.close()
    cnx.close()

# mqtt 연결
client = mqtt.Client()

# 이벤트 루프 생성
loop = asyncio.get_event_loop()

# 10초마다 실행할 함수를 등록
loop.create_task(run_every_10_seconds())

# 이벤트 루프 실행
loop.run_forever()

