#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
import urllib.request
import uuid
import time

REQUIRED_CONFIG_KEYS = ["start_date", "api_key", "user_agent"]
LOGGER = singer.get_logger()


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



def sync(config, state):
    """ Sync data from tap source """
    # Load soxhub_stream and guide_stream schemas
    schemas = load_schemas()
    singer.write_schema('nps', schemas.get('nps'),'id')
    singer.write_schema('nps_responses', schemas.get('nps_responses'),'uuid')

    # get the guide response data
    null = None
    guide_data = {
    "response": {
        "mimeType": "application/json"
    },
    "request": {
        "name": "NpsPollListAggregation-list",
        "pipeline": [
            {
                "source": {
                    "guides": null
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
                                "guideEvents": null,
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
                                            "count": null
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
                                            "count": null
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
    params = json.dumps(guide_data).encode('utf8')
    headers ={'X-Pendo-Integration-Key': '77211473-d261-47dc-5e36-dfc6e51ab92d', 'Content-Type': 'application/json'}
    req = urllib.request.Request('https://app.pendo.io/api/v1/aggregation', params, headers)

    response=(urllib.request.urlopen(req).read().decode())
    data_load = json.loads(response)
    singer.write_records('nps', data_load['results'])

    # Make a list of the available guideIds
    guideArr = []
    [guideArr.append(item['id']) for item in data_load['results']]

    # make call to get the pollIds for corresponding guideIds
    for guideId in guideArr:
        guideId = guideId
        null = None
        poll_data = {
            "response": {},
            "request": {
                "name": "pollIds",
                "pipeline": [
                    {
                        "source": {
                            "guides": null
                        }
                    },
                    {
                        "filter": "id==" + "`"+guideId+"`"
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

        params = json.dumps(poll_data).encode('utf8')
        headers ={'X-Pendo-Integration-Key': '77211473-d261-47dc-5e36-dfc6e51ab92d', 'Content-Type': 'application/json'}
        req = urllib.request.Request('https://app.pendo.io/api/v1/aggregation', params, headers)

        response=(urllib.request.urlopen(req))
        data_load = response.read().decode('utf-8')
        json_obj = json.loads(data_load)

        # Get pollIds in order to get the pollQualResponse and pollNumResponse
        pollValues = (list(json_obj['results'][0].values()))

        response_data = {
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
                                            "guideId": guideId,
                                            "pollId": pollValues[0]
                                        }
                                    }
                                },
                                {
                                    "identified": "visitorId"
                                },
                                {
                                    "select": {
                                        "visitorId": "visitorId",
                                        "accountId": "accountId",
                                        "pollNumResponse": "response",
                                        "time": "time"
                                    }
                                }
                            ],
                            [
                                {
                                    "source": {
                                        "pollsSeenEver": {
                                            "guideId": guideId,
                                            "pollId": pollValues[1]
                                        }
                                    }
                                },
                                {
                                    "identified": "visitorId"
                                },
                                {
                                    "select": {
                                        "visitorId": "visitorId",
                                        "pollQualResponse": "response",
                                        "accountId": "accountId",
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

        params = json.dumps(response_data).encode('utf8')
        headers ={'X-Pendo-Integration-Key': '77211473-d261-47dc-5e36-dfc6e51ab92d', 'Content-Type': 'application/json'}
        req = urllib.request.Request('https://app.pendo.io/api/v1/aggregation', params, headers)
        response=(urllib.request.urlopen(req).read().decode())
        data_load = json.loads(response)

        # Add a uuid and the guideId to each record
        if data_load['results']is null:
            pass
        else:
            for item in data_load['results']:
                item.update({"guideId": guideId.replace("'", "")})
                item.update({"uuid": uuid.uuid4().hex[:8]})
                if 'time' in item and item['time'] is not null:
                    item['time'] = time.strftime('%Y-%m-%d %H:%M', time.localtime(item['time']/1000))
            singer.write_records('nps_responses', data_load['results'])


@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    sync(args.config, args.state)


if __name__ == "__main__":
    main()
