import requests
from celery import Celery

app = Celery('task_events_monitoring.tasks', broker='pyamqp://guest@localhost')

@app.task
def visit(uri):
    response = requests.get(uri)
    print('Response from {0}: {1}'.format(uri, response.status_code))