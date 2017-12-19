from flask import Flask, request, Response, jsonify
from decimal import *
import time
import boto3
from boto3.dynamodb.conditions import Key
from schema import Schema, And, Use
import os


def create_app():
    app = Flask(__name__)

    return app

db = boto3.resource('dynamodb',
                    region_name="us-west-2",
                    aws_access_key_id=os.environ['aws_access_key_id'],
                    aws_secret_access_key=os.environ['aws_secret_access_key'])
app = create_app()



@app.route('/scores', methods=['GET'])
def get_all_scores():
    return scores

@app.route('/scores/game/<game>', methods=['GET'])
def get_scores_for_game(game):
    table = db.Table("scorebord")

    try:
        response = table.query(
            IndexName="game-score-index",
            KeyConditionExpression=Key('game').eq(game),
            ScanIndexForward=False
        )
    except Exception as e:
        print(format(e))
        return format(e), 500

    validated = translate_dynamo_response(response['Items'])

    scores = {}
    for score in validated:
        if scores.get(score['user']) == None:
            scores[score['user']] = score

    score_list = []
    for score in scores:
        score_list.append(scores[score])

    return format(score_list)

@app.route('/scores/user/<user>', methods=['GET'])
def get_scores_for_user(user):
    table = db.Table("scorebord")

    try:
        response = table.query(
            KeyConditionExpression=Key('user').eq(user),
            ScanIndexForward=False
        )
    except Exception as e:
        print(format(e))
        return format(e), 500

    validated = translate_dynamo_response(response['Items'])

    return format(validated)

@app.route('/scores/user/<user>/game/<game>', methods=['GET'])
def get_game_scores_for_user(user, game):
    table = db.Table("scorebord")

    try:
        response = table.query(
            IndexName="user-game-index",
            KeyConditionExpression=Key('user').eq(user) & Key('game').eq(game),
            ScanIndexForward=False
        )
    except Exception as e:
        print(format(e))
        return format(e), 500

    validated = translate_dynamo_response(response['Items'])

    return format(validated)

@app.route('/score', methods=['POST'])
def post_score():
    score_time = Decimal(time.time())
    payload = request.get_json()
    table = db.Table('scorebord')

    schema = Schema({'user_id': str,
                      'game': str,
                      'score': And(Use(int), lambda score: 0 <= score)})

    try:
        validated = schema.validate(payload)
    except Exception as e:
        return format(e), 400


    try:
        response = table.put_item(
            Item={
                 'user': validated["user_id"],
                 'score_time': score_time,
                 'game': validated["game"],
                 'score': validated["score"]
            }
        )
    except Exception as e:
        print(format(e))
        return format(e), 500

    return "success"


def translate_dynamo_response(response):
    schema = Schema([{'user': str,
                     'score': Use(str),
                     'game': str,
                     'score_time': Use(str)}])

    validated = schema.validate(response)
    return validated
