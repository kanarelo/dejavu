import os
import sys
import time
import traceback
import fingerprint
import multiprocessing

from dejavu.database import get_database, Database
import dejavu.decoder as decoder

class Dejavu(object):
    SONG_ID = "song_id"
    SONG_NAME = 'song_name'
    CONFIDENCE = 'confidence'
    MATCH_TIME = 'match_time'
    OFFSET = 'offset'
    OFFSET_SECS = 'offset_seconds'

    def __init__(self, config):
        super(Dejavu, self).__init__()

        self.config = config
        database_type = config.get("database", {}).get("database_type")

        # initialize db
        db_cls = get_database(database_type=database_type)

        self.db = db_cls(**config.get("database", {}))
        self.db.setup()

        # if we should limit seconds fingerprinted,
        # None|-1 means use entire track
        self.limit = self.config.get("fingerprint_limit", None)
        if self.limit == -1:  # for JSON compatibility
            self.limit = None
    
        # get songs previously indexed
        self.update_songs()

    def update_songs(self):
        self.songs = self.db.get_songs()
        self.songhashes_set = set()  # to know which ones we've computed before
        for song in self.songs:
            song_hash = song._asdict()[Database.FIELD_FILE_SHA1]
            self.songhashes_set.add(song_hash)

    def get_fingerprinted_songs(self):
        return self.songhashes_set

    def fingerprint_directory(self, path, extensions, nprocesses=None):
        # Try to use the maximum amount of processes if not given.
        try:
            nprocesses = nprocesses or (multiprocessing.cpu_count() - 1)
        except NotImplementedError:
            nprocesses = 1
        else:
            nprocesses = 1 if (nprocesses <= 0) else nprocesses

        pool = multiprocessing.Pool(nprocesses)

        filenames_to_fingerprint = []
        for filename, _ in decoder.find_files(path, extensions):
            # don't refingerprint already fingerprinted files
            if decoder.unique_hash(filename) in self.songhashes_set:
                print "%s already fingerprinted, continuing..." % filename
                continue
            else:
                print "Adding '%s' to Queue" % filename
                filenames_to_fingerprint.append(filename)

        if not filenames_to_fingerprint:
            print "=============" * 3
            print "All the files provided have already been fingerprinted, exiting..."
            print "=============" * 3
            return 

        # Prepare _fingerprint_worker input
        worker_input = zip(
            filenames_to_fingerprint,
            [self.limit, None, "wav"] * len(filenames_to_fingerprint))

        # Send off our tasks
        iterator = pool.imap_unordered(_fingerprint_worker, worker_input)
        total_items = len(filenames_to_fingerprint)
        songs_range = range(total_items)
        done = []

        # Loop till we have all of them
        while True:
            print "=============" * 3
            try:
                song_name, hashes, file_hash = iterator.next()
            except multiprocessing.TimeoutError:
                continue
            except StopIteration:
                break
            except:
                print("Failed fingerprint")
                # Print traceback because we can't reraise it here
                traceback.print_exc(file=sys.stdout)
            else:
                done.append(songs_range.pop(0)) 
                print "Inserting song %s of %s %s:%s to database" % (len(done), total_items, song_name, file_hash)
                sid = self.db.insert_song(song_name, file_hash)

                print "inserting hashes for song %s" % song_name
                self.db.insert_hashes(sid, hashes)
                print "updating fingerprinted song %s" % song_name
                self.db.set_song_fingerprinted(sid)

                print "updating song hashes: %s" % song_name
                self.get_fingerprinted_songs().add(file_hash)
            print "=============" * 3

        pool.close()
        pool.join()

    def fingerprint_file(self, filepath, song_name=None):
        songname = decoder.path_to_songname(filepath)
        song_hash = decoder.unique_hash(filepath)
        song_name = song_name or songname
        # don't refingerprint already fingerprinted files
        if song_hash in self.songhashes_set:
            print "%s already fingerprinted, continuing..." % song_name
        else:
            song_name, hashes, file_hash = _fingerprint_worker(
                filepath,
                self.limit,
                song_name=song_name)

            print "Inserting song %s:%s to database" % (song_name, file_hash)
            sid = self.db.insert_song(song_name, file_hash)

            print "Inserting hashes for song %s" % song_name
            self.db.insert_hashes(sid, hashes)
            
            print "Updating fingerprinted song %s" % song_name
            self.db.set_song_fingerprinted(sid)

            print "Updating song hashes: %s" % song_name
            self.get_fingerprinted_songs().add(file_hash)

    def find_matches(self, samples, Fs=fingerprint.DEFAULT_FS):
        hashes = fingerprint.fingerprint(samples, Fs=Fs)
        return self.db.return_matches(hashes)

    def align_matches(self, matches):
        """
            Finds hash matches that align in time with other matches and finds
            consensus about which hashes are "true" signal from the audio.

            Returns a dictionary with match information.
        """
        # align by diffs
        diff_counter = {}
        largest = 0
        largest_count = 0
        song_id = -1
        for tup in matches:
            sid, diff = tup
            if diff not in diff_counter:
                diff_counter[diff] = {}
            if sid not in diff_counter[diff]:
                diff_counter[diff][sid] = 0
            diff_counter[diff][sid] += 1

            if diff_counter[diff][sid] > largest_count:
                largest = diff
                largest_count = diff_counter[diff][sid]
                song_id = sid

        # extract idenfication
        song = self.db.get_song_by_id(song_id)
        if song:
            songname = song.song_name
        else:
            return None

        # return match info
        nseconds = round(float(largest) / fingerprint.DEFAULT_FS *
                         fingerprint.DEFAULT_WINDOW_SIZE *
                         fingerprint.DEFAULT_OVERLAP_RATIO, 5)
        song_dict = {
            Dejavu.SONG_ID : song_id,
            Dejavu.SONG_NAME : songname,
            Dejavu.CONFIDENCE : largest_count,
            Dejavu.OFFSET : int(largest),
            Dejavu.OFFSET_SECS : nseconds,
            Database.FIELD_FILE_SHA1 : song.file_sha1
        }
        return song_dict

    def recognize(self, recognizer, *options, **kwoptions):
        r = recognizer(self)
        return r.recognize(*options, **kwoptions)

def _fingerprint_worker(filename, limit=None, file_format="wav", song_name=None):
    # Pool.imap sends arguments as tuples so we have to unpack
    # them ourself.
    try:
        filename, limit = filename
    except ValueError:
        pass

    songname, extension = os.path.splitext(os.path.basename(filename))
    song_name = song_name or songname
    channels, Fs, file_hash = decoder.read(filename, limit, file_format)
    result = set()
    channel_amount = len(channels)

    for channeln, channel in enumerate(channels):
        # TODO: Remove prints or change them into optional logging.
        print("Fingerprint channel %d/%d for %s" % (channeln + 1,
                                                       channel_amount,
                                                       filename))
        hashes = fingerprint.fingerprint(channel, Fs=Fs, song_name=song_name)
        print("Finished channel %d/%d for %s" % (channeln + 1, channel_amount, filename))
        result |= set(hashes)

    return song_name, result, file_hash

def chunkify(lst, n):
    """
    Splits a list into roughly n equal parts.
    http://stackoverflow.com/questions/2130016/splitting-a-list-of-arbitrary-size-into-only-roughly-n-equal-parts
    """
    return [lst[i::n] for i in xrange(n)]
