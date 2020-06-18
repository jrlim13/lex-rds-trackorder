import os
import logging
import time
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

""" --- DB Connection configuration --- """

endpoint = 'logistics.cbcgpcxnvsnk.ap-southeast-1.rds.amazonaws.com'
username = 'admin'
password = 'password'
database_name = 'orders'

connection = pymysql.connect(endpoint, user=username, passwd=password, db=database_name)

""" -- Builds responses which match the structure of the dialog actions --"""

def get_slots(intent_request):
    return intent_request['currentIntent']['slots']

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }

def close(session_attributes, fulfillment_state, message):
    connection.commit()
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

def greetings(intent_request):
    return close(intent_request['sessionAttributes'],
                    'Fulfilled',
                    {
                        'contentType': 'PlainText',
                        'content': 'Hello there!'
                    })

def track_order(intent_request):
    tracking_number = get_slots(intent_request)['TrackingNumber']
    source = intent_request['invocationSource']

    cursor = connection.cursor()
    query = 'SELECT t1.* \
                    FROM orders.order_status t1 \
                    INNER JOIN \
                        (SELECT tracking_number, MAX(datetime_updated) AS MaxDateTime \
                        FROM orders.order_status \
                        GROUP BY tracking_number) t2 \
                    ON t1.tracking_number = t2.tracking_number \
                    AND t1.datetime_updated = t2.MaxDateTime \
                    WHERE t1.tracking_number = {}'.format(tracking_number)
    row_count = cursor.execute(query)

    if row_count > 0:
        rows = cursor.fetchall()

        for row in rows:
            db_tracking_number = row[0]
            db_datetime_updated = row[1]
            db_status = row[2]

        return close(intent_request['sessionAttributes'],
                        'Fulfilled',
                        {
                            'contentType': 'PlainText',
                            'content': 'Status of tracking number {} as of {}: {}.'.format(db_tracking_number, db_datetime_updated, db_status)
                        })
    else:
        return close(intent_request['sessionAttributes'],
                        'Fulfilled',
                        {
                            'contentType': 'PlainText',
                            'content': 'Tracking number {} not found in records.'.format(tracking_number)
                        })

""" --- Intents ---"""

def dispatch(intent_request):
    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))
    
    intent_name = intent_request['currentIntent']['name']

    if intent_name == 'GreetingsIntent':
        return greetings(intent_request)
    elif intent_name == 'TrackOrder':
        return track_order(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')

""" --- Main Handler ---"""

def lambda_handler(event, context):
    os.environ['TZ'] = 'Asia/Manila'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)

