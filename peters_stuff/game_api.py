#!/usr/bin/env python
import json
import random
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())


@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"


#@app.route("/purchase_a_sword")
#def purchase_a_sword():
#    purchase_sword_event = {'event_type': 'purchase_sword',
#                            'sword_size': 'large'}
#    log_to_kafka('events', purchase_sword_event)
#    return "Sword Purchased!\n"

@app.route("/buy_a_sword")
def buy_a_sword():
    buy_sword_event = {'event_type': 'buy_sword',
                       'sword_type': 'normal'}
    log_to_kafka('events', buy_sword_event)
    return "Sword Bought!\n"

@app.route("/join_guild")
def join_guild():
    guilds = ['Morgan','Lam','Kavlakoglu']
    guild = random.choice(guilds)
    join_guild_event = {'event_type': 'join_guild',
                        'guild_name': guild,
                        'assignment_type': 'random'}
    log_to_kafka('events', join_guild_event)
    return "Joined " + guild + " Guild!\n"

@app.route("/purchase_a_sharp_sword")
def purchase_a_sharp_sword():
    purchase_sword_event = {'event_type': 'buy_sword',
                            'sword_type': 'sharp'}
    log_to_kafka('events', purchase_sword_event)
    return "Sharp Sword Purchased!\n"

@app.route("/join_guild_morgan")
def join_guild_morgan():
    join_guild_event = {'event_type': 'join_guild',
                        'guild_name': 'Morgan',
                        'assignment_type': 'manual'}
    log_to_kafka('events', join_guild_event)
    return "Joined Morgan Guild!\n"

@app.route("/join_guild_lam")
def join_guild_lam():
    join_guild_event = {'event_type': 'join_guild',
                        'guild_name': 'Lam',
                        'assignment_type': 'manual'}
    log_to_kafka('events', join_guild_event)
    return "Joined Lam Guild!\n"

@app.route("/join_guild_kavlakoglu")
def join_guild_kavlakoglu():
    join_guild_event = {'event_type': 'join_guild',
                        'guild_name': 'Kavlakoglu',
                        'assignment_type': 'manual'}
    log_to_kafka('events', join_guild_event)
    return "Joined Kavlakoglu Guild!\n"