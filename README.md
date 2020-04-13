# tap-pendo

## Installation
1. Create virtual enviroment 
2. Install Packages 
  - ```$ pip install -e .```

## About this Tap
This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from [Pendo](https://app.pendo.io/)
- Extracts the following resources:
  - [NPS example](https://app.pendo.io/net-promoter-system/guides/5YLAJsKuhRAQ4mhOqUfNyuwwvWU/polls/pfx5vh7zbo?view=nps-responses)
- There are two schemas ***nps*** and ***nps_responses***
  - ***nps*** is all the guides available
  - ***nps_responses*** is where all the responses from all the given guides will be displayed
---

Copyright &copy; 2018 Stitch