from __future__ import absolute_import

import sys

from apscheduler.executors.base import BaseExecutor, run_job
from apscheduler.executors.base_py3 import run_coroutine_job
from apscheduler.util import iscoroutinefunction_partial


class AsyncIOExecutor(BaseExecutor):
    """
    Runs jobs in the default executor of the event loop.

    If the job function is a native coroutine function, it is scheduled to be run directly in the
    event loop as soon as possible. All other functions are run in the event loop's default
    executor which is usually a thread pool.

    Plugin alias: ``asyncio``
    """

    def start(self, scheduler, alias):
        super(AsyncIOExecutor, self).start(scheduler, alias)
        self._eventloop = scheduler._eventloop
        self._pending_futures = set()

    def shutdown(self, wait=True):
        # sigterm 받았을때 스케줄러가 끝날때까지 기다리도록 하기 위해서 clear 만 진행
        self._pending_futures.clear()

    def _do_submit_job(self, job, run_times):
        def callback(f):
            self._pending_futures.discard(f)
            try:
                events = f.result()
            except BaseException:
                self._run_job_error(job.id, *sys.exc_info()[1:])
            else:
                self._run_job_success(job.id, events)

        if iscoroutinefunction_partial(job.func):
            coro = run_coroutine_job(job, job._jobstore_alias, run_times, self._logger.name)
            f = self._eventloop.create_task(coro)
        else:
            f = self._eventloop.run_in_executor(None, run_job, job, job._jobstore_alias, run_times,
                                                self._logger.name)

        f.add_done_callback(callback)
        self._pending_futures.add(f)

