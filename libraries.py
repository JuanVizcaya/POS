# -*- coding: utf-8 -*-
from flask import Flask, request, redirect, send_file, make_response, abort, jsonify
import time
from datetime import timedelta
import pandas as pd
import os
from functools import wraps
import pandas.io.sql as sqlio
import psycopg2
import boto3
import pandas as pd
import io
import sys
import json
import requests
import logging
from utils_op import *
import uuid
import time
from datetime import datetime
#from slackclient import SlackClient
import traceback
#sale2?


def isclose(x, y, abs_tol):

    return abs(x-y)<=abs_tol
