from apscheduler.schedulers.blocking import BlockingScheduler
from AddNewCamera import *

sched = BlockingScheduler() 

@sched.scheduled_job('interval', minutes=3)
def timed_job():
	getWhoCalled()
    print('This job is run every three minutes.')

@sched.scheduled_job('cron', day_of_week='mon-fri', hour=17)
def scheduled_job():
	getWhoCalled()
    print('This job is run every weekday at 5pm.')

sched.start()