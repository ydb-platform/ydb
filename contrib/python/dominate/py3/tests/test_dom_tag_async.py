from asyncio import gather, run, Semaphore
from dominate.dom_tag import async_context_id
from textwrap import dedent

from dominate import tags

# To simulate sleep without making the tests take a hella long time to complete
# lets use a pair of semaphores to explicitly control when our coroutines run.
# The order of execution will be marked as comments below:
def test_async_bleed():
    async def tag_routine_1(sem_1, sem_2):
        root = tags.div(id = 1) # [1]
        with root: # [2]
            sem_2.release() # [3]
            await sem_1.acquire() # [4]
            tags.div(id = 2) # [11]
        return str(root) # [12]

    async def tag_routine_2(sem_1, sem_2):
        await sem_2.acquire() # [5]
        root = tags.div(id = 3) # [6]
        with root: # [7]
            tags.div(id = 4) # [8]
        sem_1.release() # [9]
        return str(root) # [10]

    async def merge():
        sem_1 = Semaphore(0)
        sem_2 = Semaphore(0)
        return await gather(
            tag_routine_1(sem_1, sem_2), 
            tag_routine_2(sem_1, sem_2)
        )

    # Set this test up for failure - pre-set the context to a non-None value.
    # As it is already set, _get_async_context_id will not set it to a new, unique value
    # and thus we won't be able to differentiate between the two contexts. This essentially simulates
    # the behavior before our async fix was implemented (the bleed):
    async_context_id.set(0)
    tag_1, tag_2 = run(merge())

    # This looks wrong - but its what we would expect if we don't
    # properly handle async...
    assert tag_1 == dedent("""\
        <div id="1">
          <div id="3">
            <div id="4"></div>
          </div>
          <div id="2"></div>
        </div>
    """).strip()

    assert tag_2 == dedent("""\
        <div id="3">
          <div id="4"></div>
        </div>
    """).strip()

    # Okay, now lets do it right - lets clear the context. Now when each async function
    # calls _get_async_context_id, it will get a unique value and we can differentiate.
    async_context_id.set(None)
    tag_1, tag_2 = run(merge())

    # Ah, much better...
    assert tag_1 == dedent("""\
        <div id="1">
          <div id="2"></div>
        </div>
    """).strip()
    
    assert tag_2 == dedent("""\
        <div id="3">
          <div id="4"></div>
        </div>
    """).strip()
