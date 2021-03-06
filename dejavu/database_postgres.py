""" Class for interacting with Postgres database.
"""
from itertools import izip_longest
import Queue
import sys
import binascii

try:
    import psycopg2
except ImportError as err:
    print "Module not installed", err
    sys.exit(1)

from psycopg2.extras import DictCursor, RealDictCursor, wait_select
from dejavu.database import Database

class PostgresDatabase(Database):
    """ Class to interact with Postgres databases.
    """

    type = "postgresql"

    # The number of hashes to insert at a time
    NUM_HASHES = 11250

    # Schema
    DEFAULT_SCHEMA = 'public'

    # Creates an index on fingerprint itself for webscale
    CREATE_FINGERPRINT_INDEX = """
        DO $$
            BEGIN

            IF NOT EXISTS (
                SELECT 1
                FROM   pg_class c
                JOIN   pg_namespace n ON n.oid = c.relnamespace
                WHERE  c.relname = 'fingerprint_index'
                AND    n.nspname = '%s'
            ) THEN

            CREATE INDEX fingerprint_index ON %s.%s (%s);
            END IF;
        END$$;
        """ % (DEFAULT_SCHEMA,
               DEFAULT_SCHEMA,
               Database.FINGERPRINTS_TABLENAME,
               Database.FIELD_HASH
              )

    INSERT_FINGERPRINT_BASIC = """
        INSERT INTO %s (%s, %s, %s) VALUES
        """ % (
            Database.FINGERPRINTS_TABLENAME,
            Database.FIELD_HASH,
            Database.FIELD_SONG_ID,
            Database.FIELD_OFFSET,
        )
    # Inserts (ignores duplicates)
    INSERT_FINGERPRINT = """
        %s (decode(%%s, 'hex'), %%s, %%s);
        """ % (
            INSERT_FINGERPRINT_BASIC
        )

    # Inserts song information.
    INSERT_SONG = """
        INSERT INTO %s (%s, %s)
        values (%%s, decode(%%s, 'hex'))
        RETURNING %s;
        """ % (
            Database.SONGS_TABLENAME,
            Database.FIELD_SONGNAME,
            Database.FIELD_FILE_SHA1,
            Database.FIELD_SONG_ID
        )

    # Select a single fingerprint given a hex value.
    SELECT = """
        SELECT %s, %s
        FROM %s
        WHERE %s = decode(%%s, 'hex');
        """ % (
            Database.FIELD_SONG_ID,
            Database.FIELD_OFFSET,
            Database.FINGERPRINTS_TABLENAME,
            Database.FIELD_HASH
        )

    # Selects multiple fingerprints based on hashes
    SELECT_MULTIPLE = """
        SELECT %s, %s, %s
        FROM %s
        WHERE %s IN (%%s);
        """ % (
            Database.FIELD_HASH,
            Database.FIELD_SONG_ID,
            Database.FIELD_OFFSET,
            Database.FINGERPRINTS_TABLENAME,
            Database.FIELD_HASH
        )

    # Selects all fingerprints from the fingerprints table.
    SELECT_ALL = """
        SELECT %s, %s
        FROM %s;
        """ % (
            Database.FIELD_SONG_ID,
            Database.FIELD_OFFSET,
            Database.FINGERPRINTS_TABLENAME
        )

    # Selects a given song.
    SELECT_SONG = """
        SELECT %s, %s
        FROM %s
        WHERE %s = %%s
        """ % (
            Database.FIELD_SONGNAME,
            Database.FIELD_FILE_SHA1,
            Database.SONGS_TABLENAME,
            Database.FIELD_SONG_ID
        )

    # Returns the number of fingerprints
    SELECT_NUM_FINGERPRINTS = """
        SELECT COUNT(*) as n
        FROM %s
        """ % (
            Database.FINGERPRINTS_TABLENAME
        )

    # Selects unique song ids
    SELECT_UNIQUE_SONG_IDS = """
        SELECT COUNT(DISTINCT %s) as n
        FROM %s
        WHERE %s = True;
        """ % (
            Database.FIELD_SONG_ID,
            Database.SONGS_TABLENAME,
            Database.FIELD_FINGERPRINTED
        )

    # Selects all FINGERPRINTED songs.
    SELECT_SONGS = """
        SELECT %s, %s, %s
        FROM %s WHERE %s = True;
        """ % (
            Database.FIELD_SONG_ID,
            Database.FIELD_SONGNAME,
            Database.FIELD_FILE_SHA1,
            Database.SONGS_TABLENAME,
            Database.FIELD_FINGERPRINTED
        )

    # Drops the fingerprints table (removes EVERYTHING!)
    DROP_FINGERPRINTS = """
        DROP TABLE IF EXISTS %s;""" % (
            Database.FINGERPRINTS_TABLENAME
        )

    # Drops the songs table (removes EVERYTHING!)
    DROP_SONGS = """
        DROP TABLE IF EXISTS %s;
        """ % (
            Database.SONGS_TABLENAME
        )

    # Updates a fingerprinted song
    UPDATE_SONG_FINGERPRINTED = """
        UPDATE %s
        SET %s = True
        WHERE %s = %%s
        """ % (
            Database.SONGS_TABLENAME,
            Database.FIELD_FINGERPRINTED,
            Database.FIELD_SONG_ID
        )

    # Deletes all unfingerprinted songs.
    DELETE_UNFINGERPRINTED = """
        DELETE
        FROM %s
        WHERE %s = False;
        """ % (
            Database.SONGS_TABLENAME,
            Database.FIELD_FINGERPRINTED
        )

    def __init__(self, **options):
        """ Creates the DB layout, creates connection, etc.
        """
        super(PostgresDatabase, self).__init__()
        self.cursor = cursor_factory(**options)
        self._options = options

    def after_fork(self):
        """
        Clear the cursor cache, we don't want any stale connections from
        the previous process.
        """
        Cursor.clear_cache()

    def setup(self):
        """
        Creates any non-existing tables required for dejavu to function.

        This also removes all songs that have been added but have no
        fingerprints associated with them.
        """
        with self.cursor() as cur:
            cur.execute(self.CREATE_FINGERPRINT_INDEX)

    def empty(self):
        """
        Drops tables created by dejavu and then creates them again
        by calling `PostgresDatabase.setup`.

        This will result in a loss of data, so this might not
        be what you want.
        """
        with self.cursor() as cur:
            cur.execute(self.DROP_FINGERPRINTS)
            cur.execute(self.DROP_SONGS)
        self.setup()

    def delete_unfingerprinted_songs(self):
        """
        Removes all songs that have no fingerprints associated with them.
        This might not be applicable either.
        """
        with self.cursor() as cur:
            cur.execute(self.DELETE_UNFINGERPRINTED)

    def get_num_songs(self):
        """
        Returns number of songs the database has fingerprinted.
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_UNIQUE_SONG_IDS)
            for count, in cur:
                return count
            return 0

    def get_num_fingerprints(self):
        """
        Returns number of fingerprints present.
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_NUM_FINGERPRINTS)

            for count, in cur:
                return count
            return 0

    def set_song_fingerprinted(self, sid):
        """
        Toggles fingerprinted flag to TRUE once a song has been completely
        fingerprinted in the database.
        """
        with self.cursor() as cur:
            cur.execute(self.UPDATE_SONG_FINGERPRINTED, (sid,))

    def get_songs(self):
        """
        Generator to return songs that have the fingerprinted
        flag set TRUE, ie, they are completely processed.
        """
        with self.cursor(cursor_type=RealDictCursor) as cur:
            cur.execute(self.SELECT_SONGS)
            for row in cur:
                (song_id, song_name, file_sha1) = (row['song_id'], row['song_name'], row['file_sha1'])
                yield Database.Song(song_id, song_name, binascii.hexlify(file_sha1).upper())

    def get_song_by_id(self, song_id):
        """
        Returns song by its ID.
        """
        with self.cursor(cursor_type=RealDictCursor) as cur:
            cur.execute(self.SELECT_SONG, (song_id,))

            song_obj = cur.fetchone()

            if song_obj is None:
                return
            
            song_name = song_obj.get(self.FIELD_SONGNAME)
            file_sha1 = song_obj.get(self.FIELD_FILE_SHA1)
            if file_sha1:
                return Database.Song(song_id, song_name, binascii.hexlify(file_sha1).upper())

    def insert(self, bhash, song_id, offset):
        """
        Insert a (sha1, song_id, offset) row into database.
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_FINGERPRINT, bhash, song_id, offset)

    def insert_song(self, songname, file_hash):
        """
        Inserts song in the database and returns the ID of the inserted record.
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_SONG, (songname, file_hash))
            return cur.fetchone()[0]

    def query(self, bhash):
        """
        Return all tuples associated with hash.

        If hash is None, returns all entries in the
        database (be careful with that one!).
        """
        query = self.SELECT
        if not bhash:
            query = self.SELECT_ALL

        with self.cursor() as cur:
            cur.execute(query)

            for sid, offset in cur:
                yield (sid, offset)

    def get_iterable_kv_pairs(self):
        """
        Returns all tuples in database.
        """
        return self.query(None)

    def insert_hashes(self, sid, hashes):
        """
        Insert series of hash => song_id, offset
        values into the database.
        """
        print "Inserting %s hashes for song_id %s" % (len(hashes), sid)

        values = []
        for bhash, offset in hashes:
            values.append((bhash, sid, offset))

        with self.cursor() as cur:
            for split_values in grouper(values, self.NUM_HASHES):
                args_str = ','.join(cur.mogrify("(decode(%s, 'hex'), %s, %s)", x) for x in split_values)
                cur.execute(self.INSERT_FINGERPRINT_BASIC + " " + args_str + ";")

    def return_matches(self, hashes):
        """
        Return the (song_id, offset_diff) tuples associated with
        a list of (sha1, sample_offset) values as a generator.
        """
        # Create a dictionary of hash => offset pairs for later lookups
        mapper = {}
        for bhash, offset in hashes:
            mapper[bhash.upper()] = offset

        # Get an iteratable of all the hashes we need
        values = mapper.keys()

        with self.cursor() as cur:
            for split_values in grouper(values, self.NUM_HASHES):
                # Create our IN part of the query
                query = self.SELECT_MULTIPLE
                query = query % ', '.join(["decode(%s, 'hex')"] * len(split_values))

                cur.execute(query, split_values)

                for bhash, sid, offset in cur:
                    bhash = binascii.hexlify(bhash).upper()
                    # (sid, db_offset - song_sampled_offset)
                    yield (sid, offset - mapper[bhash])

    def __getstate__(self):
        return (self._options,)

    def __setstate__(self, state):
        self._options, = state
        self.cursor = cursor_factory(**self._options)

def grouper(iterable, num, fillvalue=None):
    """ Groups values.
    """
    args = [iter(iterable)] * num
    return (filter(None, values) for values
            in izip_longest(fillvalue=fillvalue, *args))


def cursor_factory(**factory_options):
    """ Initializes the cursor, ex passes hostname, port,
    etc.
    """
    def cursor(**options):
        """ Builds a cursor.
        """
        options.update(factory_options)
        return Cursor(**options)
    return cursor


class Cursor(object):
    """
    Establishes a connection to the database and returns an open cursor.


    ```python
    # Use as context manager
    with Cursor() as cur:
        cur.execute(query)
    ```
    """
    _cache = Queue.Queue(maxsize=5)

    def __init__(self, async=False, cursor_type=DictCursor, **options):
        super(Cursor, self).__init__()

        conn = self.get_connection(async=async, **options)
        # Ping the connection before using it from the cache.
        conn.cursor().execute('SELECT 1')

        self.conn = conn
        self.cursor_type = cursor_type

    def get_connection(self, async=False, **options):
        def connect():
            kwargs = dict(
                dbname=options.get('db'), 
                user=options.get('user'), 
                host=options.get('host'), 
                port=options.get('port'),
                password=options.get('passwd'))

            if async is True:
                kwargs.update(async=async)
            return psycopg2.connect(**kwargs)

        if async is False:
            try:
                return self._cache.get_nowait()
            except Queue.Empty:
                return connect()
        else:
            return connect()

    @classmethod
    def clear_cache(cls):
        """ Clears the cache.
        """
        cls._cache = Queue.Queue(maxsize=5)

    def __enter__(self):
        self.cursor = self.conn.cursor(cursor_factory=self.cursor_type)
        return self.cursor

    def __exit__(self, extype, exvalue, traceback):
        # if we had a Postgres related error we try to rollback the cursor.
        if extype in [psycopg2.DatabaseError, psycopg2.InternalError]:
            self.conn.rollback()

        self.cursor.close()
        self.conn.commit()

        # Put it back on the queue
        try:
            self._cache.put_nowait(self.conn)
        except Queue.Full:
            self.conn.close()