import sys

from task_events_monitoring.tasks import visit

if __name__ == '__main__':
    visit.apply_async((sys.argv[1],), {'uri_params': [('q', 'celery')]}, countdown=3, exchange="celery")