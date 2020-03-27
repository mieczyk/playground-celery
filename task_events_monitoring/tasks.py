import requests
from celery import Celery

app = Celery('task_events_monitoring.tasks', broker='pyamqp://guest@localhost')
app.conf.task_send_sent_event=True

@app.task(autoretry_for=(Exception,), retry_kwargs={'max_retries': 2})
def visit(uri, uri_params=[]):
    response = requests.get(uri, params=uri_params)
    response.raise_for_status()
    
    print('Response from {0}: {1}'.format(uri, response.status_code))

    return response.status_code