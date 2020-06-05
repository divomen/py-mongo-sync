import sys
import time
import pymongo
import bson
from pymongo import errors

from mongosync import mongo_utils
from mongosync.config import MongoConfig
from mongosync.logger import Logger

log = Logger.get()


class MongoHandler(object):
    def __init__(self, conf):
        if not isinstance(conf, MongoConfig):
            raise Exception('expect MongoConfig')
        self._conf = conf
        self._mc = None

    def __del__(self):
        self.close()

    def connect(self):
        """ Connect to server.
        """
        try:
            if isinstance(self._conf.hosts, unicode):
                host, port = mongo_utils.parse_hostportstr(self._conf.hosts)
                self._mc = mongo_utils.connect(host, port, ssl=self._conf.ssl,
                                               authdb=self._conf.authdb,
                                               username=self._conf.username,
                                               password=self._conf.password)
                self._mc.admin.command('ismaster')
                return True
            else:
                log.error('hosts contains something unsupported %r' % self._conf.hosts)
        except Exception as e:
            log.error('connect failed: %s' % e)
            return False

    def reconnect(self):
        """ Try to reconnect until success.
        """
        while True:
            try:
                self.close()
                self.connect()
                self.client().admin.command('ismaster')
                return
            except Exception as e:
                log.error('reconnect failed: %s' % e)
                time.sleep(1)

    def close(self):
        """ Close connection.
        """
        if self._mc:
            self._mc.close()
            self._mc = None

    def client(self):
        return self._mc

    def create_index(self, dbname, collname, keys, **options):
        """ Create index.
        """
        while True:
            try:
                self._mc[dbname][collname].create_index(keys, **options)
                return
            except pymongo.errors.AutoReconnect as e:
                log.error('%s' % e)
                self.reconnect()

    def bulk_write(self, dbname, collname, reqs, ordered=True, ignore_duplicate_key_error=False, print_log=False):
        """ Bulk write until success.
        """
        # if print_log:
        #     log.info('Process %d ops on %s.%s' % (len(reqs), dbname, collname))
        while True:
            try:
                self._mc[dbname][collname].bulk_write(reqs,
                                                      ordered=ordered,
                                                      bypass_document_validation=False)
                if print_log:
                    log.info('Processed %d ops on %s.%s' % (len(reqs), dbname, collname))
                return
            except pymongo.errors.AutoReconnect as e:
                log.error('%s' % e)
                self.reconnect()
            except Exception as e:
                log.error('bulk write failed: %s, retry one by one' % e)
                # retry to write one by one
                for req in reqs:
                    while True:
                        try:
                            # log.info(req)
                            if isinstance(req, pymongo.ReplaceOne):
                                self._mc[dbname][collname].replace_one(req._filter, req._doc, upsert=req._upsert)
                            elif isinstance(req, pymongo.InsertOne):
                                self._mc[dbname][collname].insert_one(req._doc)
                            elif isinstance(req, pymongo.UpdateOne):
                                self._mc[dbname][collname].update_one(req._filter, req._doc, upsert=req._upsert)
                            elif isinstance(req, pymongo.DeleteOne):
                                self._mc[dbname][collname].delete_one(req._filter)
                            else:
                                log.error('invalid req: %s' % req)
                                sys.exit(1)
                            break
                        except pymongo.errors.AutoReconnect as e:
                            log.error('%s' % e)
                            self.reconnect()
                            continue
                        except pymongo.errors.DuplicateKeyError as e:
                            if ignore_duplicate_key_error:
                                log.info('ignore duplicate key error: %s: %s' % (e, req))
                                break
                            else:
                                log.error('%s: %s' % (e, req))
                                sys.exit(1)
                        except Exception as e:
                            # generally it's an odd oplog that program cannot process
                            # so abort it and bugfix
                            log.error('%s when executing %s on %s.%s' % (e, req, dbname, collname))
                            sys.exit(1)
                if print_log:
                    log.info('Processed %d ops on %s.%s, one by one' % (len(reqs), dbname, collname))

    # UpdateOne({
    #     '_id': ObjectId('5e56c076b61867f68c7eb410')
    # },
    #     SON([
    #         (u'$v', 1),
    #         (u'$set', SON([
    #                 (u'company', u'Douglas Elliman'),
    #                 (u'updatedAt', datetime.datetime(2020, 5, 29, 11, 32, 42, 183000))
    #             ])
    #          )
    #     ]), False, None)
    def tail_oplog(self, start_optime=None, await_time_ms=None):
        """ Return a tailable curosr of local.oplog.rs from the specified optime.
        """
        # set codec options to guarantee the order of keys in command
        coll = self._mc['local'].get_collection('oplog.rs',
                                                codec_options=bson.codec_options.CodecOptions(
                                                    document_class=bson.son.SON))
        # todo remove aptbot
        cursor = coll.find(
            {'fromMigrate': {'$exists': False}, 'ns': {'$ne': 'aptbot.feed'}, 'ts': {'$gte': start_optime}},
            cursor_type=pymongo.cursor.CursorType.TAILABLE_AWAIT,
            no_cursor_timeout=True)
        # New in version 3.2
        # src_version = mongo_utils.get_version(self._mc)
        # if mongo_utils.version_higher_or_equal(src_version, '3.2.0'):
        #     cursor.max_await_time_ms(1000)
        return cursor

    def apply_oplog(self, oplog, ignore_duplicate_key_error=False, print_log=False):
        """ Apply oplog.
        """
        dbname, collname = mongo_utils.parse_namespace(oplog['ns'])
        while True:
            try:
                op = oplog['op']  # 'n' or 'i' or 'u' or 'c' or 'd'
                if op == 'i':  # insert
                    if '_id' in oplog['o']:
                        self._mc[dbname][collname].replace_one({'_id': oplog['o']['_id']}, oplog['o'], upsert=True)
                    else:
                        # create index
                        # insert into db.system.indexes
                        self._mc[dbname][collname].insert(oplog['o'], check_keys=False)
                elif op == 'u':  # update
                    self._mc[dbname][collname].update(oplog['o2'], oplog['o'])
                elif op == 'd':  # delete
                    self._mc[dbname][collname].delete_one(oplog['o'])
                elif op == 'c':  # command
                    # FIX ISSUE #4 and #5
                    # if use '--colls' option to sync target collections,
                    # running command that belongs to exclusive brother collections in the same database may failed.
                    # Just skip it.
                    try:
                        self._mc[dbname].command(oplog['o'])
                    except pymongo.errors.OperationFailure as e:
                        log.info('%s: %s' % (e, oplog))
                elif op == 'n':  # no-op
                    pass
                else:
                    log.error('invalid op: %s' % oplog)
                return
            except pymongo.errors.AutoReconnect as e:
                self.reconnect()
                continue
            except pymongo.errors.DuplicateKeyError as e:
                if ignore_duplicate_key_error:
                    log.info('ignore duplicate key error: %s :%s' % (e, oplog))
                    break
                else:
                    log.error('%s: %s' % (e, oplog))
                    sys.exit(1)
            except pymongo.errors.WriteError as e:
                log.error('%s' % e)

                # For case:
                #   Update the values of shard key fields when syncing from replica set to sharded cluster.
                #
                # Once you shard a collection, the shard key and the shard key values are immutable.
                # Reference: https://docs.mongodb.com/manual/core/sharding-shard-key/
                if self._mc.is_mongos and oplog['op'] == 'u' and 'the (immutable) field' in str(e):
                    old_doc = self._mc[dbname][collname].find_one(oplog['o2'])
                    if not old_doc:
                        log.error('replay update failed: document not found:', oplog['o2'])
                        sys.exit(1)
                    if '$set' in oplog['o']:
                        new_doc = old_doc.update(oplog['o']['$set'])
                    else:
                        new_doc = oplog['o']

                    # TODO: here need a transaction to delete old and insert new

                    # delete old document
                    res = self._mc[dbname][collname].delete_one(oplog['o2'])
                    if res.deleted_count != 1:
                        log.error('replay update failed: delete old document failed:', oplog['o2'])
                        sys.exit(1)
                    # insert new document
                    res = self._mc[dbname][collname].insert_one(new_doc)
                    if not res.inserted_id:
                        log.error('replay update failed: insert new document failed:', new_doc)
                        sys.exit(1)
