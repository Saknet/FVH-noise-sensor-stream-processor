from ksql import KSQLAPI
import time

client = KSQLAPI('https://kafka11.fvh.fi:8088/')


def count_daily_average():
    query = client.query('SELECT * FROM DB-observations')
    sum_of_results = 0
    count = 0
    ts = time.time()
    secondsinday = 24 * 60 * 60

    for item in query:
        if item.resulttime > ts - secondsinday:
            count += 1
            sum_of_results += float(item.result)
    
    if (count > 0):
        return sum_of_results / count
    else:
        return 0   
    
def location_query(sensor_link):
    query = client.query('SELECT thingstolocation.location_link FROM thingstolocation ttl LEFT JOIN datastream ds ON ttl.thing_link = ds.thing_link WHERE ds.sensor_link = :sensor_link')
    location = None
    for item in query:
        location = item
    return location