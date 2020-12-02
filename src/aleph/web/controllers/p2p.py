import asyncio
import logging

import sentry_sdk
from aiohttp import web

from aleph.services.ipfs.pubsub import pub as pub_ipfs
from aleph.services.p2p import pub as pub_p2p
from aleph.web import app

LOGGER = logging.getLogger('web.controllers.p2p')

async def pub_json(request):
    """ Forward the message to P2P host and IPFS server as a pubsub message
    """
    data = await request.json()
    status = "success"
    try:
        if app['config'].ipfs.enabled.value:
            await asyncio.wait_for(
                pub_ipfs(data.get('topic'), data.get('data')), .2)
    except Exception as e:
        LOGGER.exception("Can't publish on ipfs")
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush()
        raise
        status = "warning"
    
    
    try:
        await asyncio.wait_for(
            pub_p2p(data.get('topic'), data.get('data')), .5)
    except Exception as e:
        LOGGER.exception("Can't publish on p2p")
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush()
        raise
        status = "warning"

    output = {
        'status': status
    }
    return web.json_response(output)

app.router.add_post('/api/v0/ipfs/pubsub/pub', pub_json)
app.router.add_post('/api/v0/p2p/pubsub/pub', pub_json)
