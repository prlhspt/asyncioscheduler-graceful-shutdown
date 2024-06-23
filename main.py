import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
import inspect
import logging
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.executors.base_py3 import run_coroutine_job

from util.executor_util import AsyncIOExecutor

logging.basicConfig(level=logging.DEBUG)

executors = {
    "default": AsyncIOExecutor(),
}

exit_stack = AsyncExitStack()


async def print_test1():
    await asyncio.sleep(5)


async def print_test2():
    await asyncio.sleep(6)


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = AsyncIOScheduler(executors=executors)
    # scheduler.start()

    # scheduler.add_job(func=print_test1, trigger=IntervalTrigger(seconds=3))
    # scheduler.add_job(func=print_test2, trigger=IntervalTrigger(seconds=3))

    yield
    # scheduler.shutdown()

    while True:
        is_end = True
        for task in asyncio.all_tasks():
            if (
                inspect.iscoroutine(task.get_coro())
                and task.get_coro().cr_code is run_coroutine_job.__code__
            ):
                is_end = False
                await asyncio.sleep(0.1)
                continue

        if is_end:
            break


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def pppp():
    await asyncio.sleep(10)
    return {"msg": "good"}
