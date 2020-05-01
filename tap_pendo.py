#!/usr/bin/env python3
import os
import json
import singer
import uuid
import datetime
import requests

from singer import utils
from singer.schema import Schema

session = requests.Session()
logger = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["api_token"]
PENDO_API_ENDPOINT = "https://app.pendo.io/api/v1/aggregation"


class AuthException(Exception):
    pass


class NotFoundException(Exception):
    pass


class InvalidHTTPMethodException(Exception):
    pass


class GuideResource():
    def __init__(self, id, poll_1_id=None, poll_2_id=None):
        self.id = id
        self.poll_1_id = poll_1_id
        self.poll_2_id = poll_2_id


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)
    return schemas


def authed_req(url, method, body={}, headers={}):
    resp = None
    session.headers.update(headers)
    if method == 'GET':
        resp = session.request(method='get', url=url)
    elif method == 'POST':
        resp = session.request(method='post', data=body, url=url)
    else:
        raise InvalidHTTPMethodException(resp.text)
    if resp.status_code == 401:
        raise AuthException(resp.text)
    if resp.status_code == 403:
        raise AuthException(resp.text)
    if resp.status_code == 404:
        raise NotFoundException(resp.text)
    return resp


def sync_guides_and_return_ids():
    guide_data = {
        "response": {
            "mimeType": "application/json"
        },
        "request": {
            "name": "NpsPollListAggregation-list",
            "pipeline": [
                {
                    "source": {
                        "guides": None
                    }
                },
                {
                    "unwind": {
                        "field": "polls"
                    }
                },
                {
                    "filter": "polls.attributes.type==\"NPSRating\""
                },
                {
                    "unwind": {
                        "field": "steps"
                    }
                },
                {
                    "unwind": {
                        "field": "steps.pollIds"
                    }
                },
                {
                    "eval": {
                        "guideId": "id",
                        "pollId": "polls.id",
                        "guideStepId": "steps.id",
                        "stepPollId": "steps.pollIds"
                    }
                },
                {
                    "filter": "pollId == stepPollId"
                },
                {
                    "merge": {
                        "fields": [
                            "guideId",
                            "guideStepId"
                        ],
                        "mappings": {
                            "numViews": "numViews"
                        },
                        "pipeline": [
                            {
                                "source": {
                                    "guideEvents": None,
                                    "timeSeries": {
                                        "period": "dayRange",
                                        "first": "now()",
                                        "count": -30
                                    }
                                }
                            },
                            {
                                "identified": "visitorId"
                            },
                            {
                                "filter": "type==\"guideSeen\""
                            },
                            {
                                "group": {
                                    "group": [
                                        "guideId",
                                        "guideStepId"
                                    ],
                                    "fields": [
                                        {
                                            "numViews": {
                                                "count": None
                                            }
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                },
                {
                    "merge": {
                        "fields": [
                            "guideId",
                            "pollId"
                        ],
                        "pipeline": [
                            {
                                "source": {
                                    "pollEvents": {},
                                    "timeSeries": {
                                        "period": "dayRange",
                                        "first": "now()",
                                        "count": -30
                                    }
                                }
                            },
                            {
                                "identified": "visitorId"
                            },
                            {
                                "filter": "isNumber(pollResponse)"
                            },
                            {
                                "eval": {
                                    "isPromoter": "if(pollResponse >= 9, 1, 0)",
                                    "isNeutral": "if(pollResponse >= 7 && pollResponse < 9, 1, 0)",
                                    "isDetractor": "if(pollResponse < 7, 1, 0)"
                                }
                            },
                            {
                                "group": {
                                    "group": [
                                        "guideId",
                                        "pollId"
                                    ],
                                    "fields": [
                                        {
                                            "numPromoters": {
                                                "sum": "isPromoter"
                                            }
                                        },
                                        {
                                            "numNeutral": {
                                                "sum": "isNeutral"
                                            }
                                        },
                                        {
                                            "numDetractors": {
                                                "sum": "isDetractor"
                                            }
                                        },
                                        {
                                            "numResponses": {
                                                "count": None
                                            }
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                },
                {
                    "select": {
                        "id": "id",
                        "name": "name",
                        "group": "group",
                        "groupId": "groupId",
                        "guideStepId": "guideStepId"
                    }
                }
            ],
            "requestId": "NpsPollListAggregation-list"
        }
    }
    # make request to get guide names and guideIds
    body = json.dumps(guide_data).encode('utf8')
    res = authed_req(PENDO_API_ENDPOINT, 'POST', body)
    json_res = res.json()

    logger.info(f"Pendo guide fetch response: {res.status_code}")
    singer.write_records('guides', json_res['results'])
    return [item['id'] for item in json_res['results']]


def get_poll_ids_for_guide(guide_id):
    """ Grab the 2 poll_ids for the passed in Pendo guide_id """
    polls_query = {
        "response": {},
        "request": {
            "name": "pollIds",
            "pipeline": [
                {
                    "source": {
                        "guides": None
                    }
                },
                {
                    "filter": f"id==`{guide_id}`"
                },
                {
                    "select": {
                        "pollId1": "polls[0].id",
                        "pollId2": "polls[1].id"
                    }
                }
            ],
            "requestId": "pollIds"
        }
    }
    body = json.dumps(polls_query).encode('utf8')
    res = authed_req(PENDO_API_ENDPOINT, 'POST', body)
    json_res = res.json()
    poll_1_id = json_res['results'][0]['pollId1']
    poll_2_id = json_res['results'][0]['pollId2']
    return GuideResource(guide_id, poll_1_id, poll_2_id)



def get_nps_responses_for_poll(guide_resource):
    """ Fetches and returns aggregated nps responses
    for a particular poll 1 & poll 2 relationship """
    nps_query = {
        "response": {
            "mimeType": "application/json"
        },
        "request": {
            "name": "pollsSeen",
            "pipeline": [
                {
                    "spawn": [
                        [
                            {
                                "source": {
                                    "pollsSeenEver": {
                                        "guideId": guide_resource.id,
                                        "pollId": guide_resource.poll_1_id
                                    }
                                }
                            },
                            {
                                "identified": "visitorId"
                            },
                            {
                                "select": {
                                    "guideId": "guideId",
                                    "visitorId": "visitorId",
                                    "accountId": "accountId",
                                    "pollNumResponse": "response",
                                    "agent": "agent",
                                    "time": "time"
                                }
                            }
                        ],
                        [
                            {
                                "source": {
                                    "pollsSeenEver": {
                                        "guideId": guide_resource.id,
                                        "pollId": guide_resource.poll_2_id
                                    }
                                }
                            },
                            {
                                "identified": "visitorId"
                            },
                            {
                                "select": {
                                    "guideId": "guideId",
                                    "visitorId": "visitorId",
                                    "pollQualResponse": "response",
                                    "accountId": "accountId",
                                    "agent": "agent"
                                }
                            }
                        ]
                    ]
                },
                {
                    "join": {
                        "fields": [
                            "visitorId",
                            "accountId"
                        ],
                        "width": 2
                    }
                }
            ],
            "requestId": "NPS_Responses"
        }
    }
    body = json.dumps(nps_query).encode('utf8')
    res = authed_req(PENDO_API_ENDPOINT, 'POST', body)
    json_res = res.json()
    results = json_res['results']

    if results:
        for item in results:
            item.update({"uuid": uuid.uuid4().hex[:8]})
            item['time'] = datetime.datetime.fromtimestamp(item['time']/1000).isoformat()
        singer.write_records('nps_responses', results)


def sync(config, state):
    """ Sync data from pendo """
    # Load soxhub_stream and guide_stream schemas
    PENDO_INTEGRATION_KEY = config['api_token']

    # default headers to use when interfacing with pendo API
    session.headers.update({
      'X-Pendo-Integration-Key': PENDO_INTEGRATION_KEY,
      'Content-Type': 'application/json'
    })

    schemas = load_schemas()
    singer.write_schema('guides', schemas.get('guides'), 'id')
    singer.write_schema('nps_responses', schemas.get('nps_responses'), 'uuid')

    guide_ids = sync_guides_and_return_ids()
    guide_resources = []
    # get all poll_ids for guides
    for guide_id in guide_ids:
        guide_resources.append(get_poll_ids_for_guide(guide_id))

    for guide_resource in guide_resources:
        get_nps_responses_for_poll(guide_resource)
    logger.info(f"finished with: ${len(guide_resources)} guide responses")


@utils.handle_top_exception(logger)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    sync(args.config, args.state)


if __name__ == "__main__":
    main()
