from logging import getLogger

try:
    from pymongo import MongoClient
except ImportError:  # pragma: no cover
    # Backward compatibility with PyMongo 2.2
    from pymongo import Connection as MongoClient

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket

LOGGER = getLogger('model')

db_backend = None

# Mongodb connection and db
connection = None
db = None
fs = None


def init_mongodb(uri, database, ensure_indexes=True):
    global connection, db, fs
    connection = AsyncIOMotorClient(uri,
                                    tz_aware=True)
    db = connection[database]
    fs = AsyncIOMotorGridFSBucket(db)
    sync_connection = MongoClient(uri,
                                  tz_aware=True)
    sync_db = sync_connection[database]

    if ensure_indexes:
        LOGGER.info('Inserting indexes')
        from aleph.model.messages import Message
        Message.ensure_indexes(sync_db)
        from aleph.model.pending import PendingMessage, PendingTX
        PendingMessage.ensure_indexes(sync_db)
        PendingTX.ensure_indexes(sync_db)
        from aleph.model.chains import Chain
        Chain.ensure_indexes(sync_db)
        from aleph.model.p2p import Peer
        Peer.ensure_indexes(sync_db)
        # from aleph.model.hashes import Hash
        # Hash.ensure_indexes(sync_db)


def init_db(config, ensure_indexes=True):
    init_mongodb(uri=config.mongodb.uri.value,
                 database=config.mongodb.database.value,
                 ensure_indexes=ensure_indexes)
