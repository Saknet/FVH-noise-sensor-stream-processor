from confluent_kafka.avro import AvroProducer
from ksql_queries import location_query
import requests                               

def produce_messages(msg):
    producer311 = create_kafka_avro_producer()
    observations = msg.value['observations']

    result = observations.value
    time = observations.timestamp
    produce_db_observations(result, time)

    if (float(result) > 70):
        sensor = msg.value['sensor']
        sensor_link = create_sensor_link(sensor)
        location_link = location_query(sensor_link)
        location = get_location(location_link)
        latitute = location['latitude']
        longitude = location['longitude']
        create_open311_request(latitute, longitude)

def create_sensor_link(sensor):
    sensor_link = ""
    return sensor_link        

def get_location(location):
    location = requests.get(location)
    return location.json()

def create_open311_request(latitude, longitude):
    api_endpoint = 'https://asiointi.hel.fi/palautews/rest/v1/requests.json'
    api_key = ''
    service_code = 123
    description = 'senor is broken'
    
    data = {'api_key': api_key, 
            'service_code': service_code, 
            'description' : description,
            'lat': latitude, 
            'long': longitude} 
  
    r = requests.post(url = api_endpoint, data = data)

def produce_open311_request(latitude, longitude):
    open311Producer = create_kafka_avro_producer()
    value = {"api_key": "somekey", "service_code": 123, "description": "sensor broken", "lat": {"double": latitude}, "long": {"double": longitude}}
    key = None

    value_schema_str = """{
        "type": "record",
        "namespace": "open311",
        "name": "post_service_request",
        "fields": [ 
            { "name": "api_key", "type": "string"},
            { "name": "service_code", "type": "int"},
            { "name": "description", "type": "string", "doc": "This is free form text having min 10 and max 5,000 characters. This may contain line breaks, but not html or code." },                    
            { "name": "title", "type": ["null", "string"], "default": None},
            { "name": "lat", "type": ["null", "double"], "default": None, "doc": "Currently all service types require location, either lat/long or service_object_id." },
            { "name": "long", "type": ["null", "double"], "default": None, "doc": "Currently all service types require location, either lat/long or service_object_id" },
            { "name": "service_object_type", "type": ["null", "string"], "default": None, "doc": "If service_object_id is included in the request, then service_object_type must be included."},          
            { "name": "service_object_id", "type": ["null", "int", "string"], "default": None, "doc": "If service_object_id is included in the request, then service_object_type must be included."},          
            { "name": "address_string", "type": ["null", "string"], "default": None, "doc": "This is required if no lat/long or address_id are provided. This should be written from most specific to most general geographic unit, eg address number or cross streets, street name, neighborhood/district, city/town/village, county, postal code" },
            { "name": "email", "type": ["null", "string"], "default": None, "doc": "If email address is given, service will notify the email address when service request is processed."},
            { "name": "phone", "type": ["null", "string"], "default": None },
            { "name": "first_name", "type": ["null", "double"], "default": None},
            { "name": "last_name", "type": ["null", "string"], "default": None },
            { "name": "description", "type": ["null", "string"], "default": None },
            { "name": "media_url", "type": ["null", "string"], "default": None, "doc": "API supports the most typical media formats like jpg, png, gif,.. Note, that URL links must end with a correct filetype like .jpg. Http and https URLs are supported" }
        ] 
    }"""

    open311Producer.produce(topic='open311-request', value=value, key=key)
    open311Producer.flush()

def produce_db_observations(result, time):
    dBProducer = create_kafka_avro_producer()
    value = {"phenomenontime_begin": None, "phenomenontime_end": None, "resulttime": {"long": time}, "result": {"string": result}, "resultquality": None, "validtime_begin": None, "validtime_end": None, "parameters": None, "datastream_id": None, "featureofintrest_link": None}
    key = None

    value_schema_str = """{
        "type": "record",
        "namespace": "finest_platform",
        "name": "observation",
        "fields": [
            { "name": "phenomenontime_begin", "type" : ["null", { "type" : "long", "logicalType" : "timestamp-millis" }], "default" : null  },
            { "name": "phenomenontime_end", "type" : ["null", { "type" : "long", "logicalType" : "timestamp-millis" }], "default" : null  },        
            { "name": "resulttime", "type" : ["null", { "type" : "long", "logicalType" : "timestamp-millis" }], "default" : null  },
            { "name": "result", "type": ["null", "string"], "default": null },
            { "name": "resultquality", "type": ["null", "string"], "default": null },
            { "name": "validtime_begin", "type" : ["null", { "type" : "long", "logicalType" : "timestamp-millis" }], "default" : null  },
            { "name": "validtime_end", "type" : ["null", { "type" : "long", "logicalType" : "timestamp-millis" }], "default" : null  },
            { "name": "parameters", "type": {
                "type": "record",
                "name": "parameters",
                "fields": [
                    { "name": "parameter1", "type": ["null", "string"], "default": null },
                    { "name": "parameter2", "type": ["null", "string"], "default": null }], "default": []
                }
            },
            { "name": "datastream_id", "type": ["null", "long"], "default": null },      
            { "name": "featureofintrest_link", "type": ["null", "string"], "default": null }
        ]
    }"""

    dBProducer.produce(topic='DB-observation', value=value, key=key)
    dBProducer.flush()

def create_kafka_avro_producer(key_schema, value_schema):
    return AvroProducer({
        'bootstrap.servers': '127.0.0.1:9092',
        'schema.registry.url': 'https://kafka11.fvh.fi:8081/'
    }, default_key_schema = key_schema, default_value_schema = value_schema)