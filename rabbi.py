# -*- coding: utf-8 -*-
# import mutagen
import logging
import os
import subprocess
import re
import string
import glob
import uuid
import multiprocessing
from shutil import rmtree
from datetime import datetime

import gpod


# To do that, run "sudo lsusb -v | grep -i Serial" (without the "") with your iPod plugged in,
# this should print a 16 character long string like 00A1234567891231.
# Once you have that number, create/edit /mnt/ipod/iPod_Control/Device/SysInfo
# Add to that file the line below:
# FirewireGuid: 0xffffffffffffffff

# TODO: Ctrl + C handling, process/skipped counting, free space cheking, socket for status checking, web

from os.path import splitext, isfile
import audiotools
import gio
from gtk import gdk
from beets.mediafile import MediaFile

processed = 0
skipped = 0
ru_alphabet = u"абвгдеёжзийклмнопрстуфхцчшщьыъяюэ"

_log = logging.getLogger(__name__)


def translit_to_eng(string):
    capital_letters = {u'А': u'A', u'Б': u'B', u'В': u'V', u'Г': u'G', u'Д': u'D',
                       u'Е': u'E', u'Ё': u'E', u'Ж': u'Zh', u'З': u'Z', u'И': u'I',
                       u'Й': u'Y', u'К': u'K', u'Л': u'L', u'М': u'M', u'Н': u'N',
                       u'О': u'O', u'П': u'P', u'Р': u'R', u'С': u'S', u'Т': u'T',
                       u'У': u'U', u'Ф': u'F', u'Х': u'H', u'Ц': u'Ts', u'Ч': u'Ch',
                       u'Ш': u'Sh', u'Щ': u'Sch', u'Ъ': u'', u'Ы': u'Y', u'Ь': u'',
                       u'Э': u'E', u'Ю': u'Yu', u'Я': u'Ya', }
    lower_case_letters = {u'а': u'a', u'б': u'b', u'в': u'v', u'г': u'g', u'д': u'd',
                          u'е': u'e', u'ё': u'e', u'ж': u'zh', u'з': u'z', u'и': u'i',
                          u'й': u'y', u'к': u'k', u'л': u'l', u'м': u'm', u'н': u'n',
                          u'о': u'o', u'п': u'p', u'р': u'r', u'с': u's', u'т': u't',
                          u'у': u'u', u'ф': u'f', u'х': u'h', u'ц': u'ts', u'ч': u'ch',
                          u'ш': u'sh', u'щ': u'sch', u'ъ': u'', u'ы': u'y', u'ь': u'',
                          u'э': u'e', u'ю': u'yu', u'я': u'ya', }

    len_str = len(string)

    translit_string = u""

    for index, char in enumerate(string, 1):
        repl = lower_case_letters.get(char)
        if repl:
            translit_string += repl
            continue
        repl = capital_letters.get(char)
        if repl:
            if len_str > index:
                if string[index] not in lower_case_letters:
                    repl = repl.upper()
            else:
                repl = repl.upper()
        else:
            repl = char
        translit_string += repl

    return translit_string


def RecreateIpodDB(mountpoint, ipod_name):
    def killfolder(path):
        fullpath = os.path.join(mountpoint, "iPod_control", path)
        if os.path.isdir(fullpath):
            try:
                rmtree(fullpath, ignore_errors=True)
            except OSError:
                "Can't delete %s", fullpath

    start = datetime.now()
    _log.info('Removing all artwork from %r', mountpoint)
    killfolder("Artwork")
    _log.info('Removing all music, artwork and DB from %r', mountpoint)
    killfolder("Music")
    _log.info('Removing all artwork from %r', mountpoint)
    killfolder("iTunes")

    _log.info('Creating new empty ipod filesystem at %r', mountpoint)
    gpod.itdb_init_ipod(mountpoint, None, "IPOD", None)
    # gpod.itdb_init_ipod(mountpoint, "MA450", "IPOD", None)
    _log.info('Done in %s seconds!', (datetime.now() - start).seconds)


def get_fs_freespace(pathname):
    "Get the free space of the filesystem containing pathname"
    stat = os.statvfs(pathname)
    # use f_bfree for superuser, or f_bavail if filesystem
    # has reserved space for superuser
    return stat.f_bfree * stat.f_bsize / (1024 * 1024)


def AddTrack(cpu, path):
    # track = gpod.itdb_track_new()
    # _log.info('Adding %s', path)
    track = dict(filetype='', title='', album='', artist='', albumartist='', genre='', composer='', tracklen=0, cd_nr=0,
                 cds=0, track_nr=0, tracks=0, year=0, bitrate=0, compilation=False, cover='', path='')

    if splitext(path)[1] == '.mp3':
        track['filetype'] = 'MP3-file'
    else:
        track['filetype'] = 'M4A-file'

    try:
        f = MediaFile(path)
    except:
        _log.error('Error reading: %r', path)
        return

    track['title'] = f.title.encode('utf-8')

    if f.disctitle:
        track['album'] = f.disctitle.encode('utf-8')
    else:
        track['album'] = f.album.encode('utf-8')
    track['artist'] = f.artist.encode('utf-8')
    track['albumartist'] = f.albumartist.encode('utf-8')
    if f.genre is not None: track['genre'] = f.genre.encode('utf-8')

    track['composer'] = track['albumartist']
    track['tracklen'] = f.length * 1000

    if f.disc: track['cd_nr'] = f.disc
    if f.disctotal: track['cds'] = f.disctotal
    if f.track: track['track_nr'] = f.track
    if f.tracktotal: track['tracks'] = f.tracktotal
    if f.year: track['year'] = f.year
    track['bitrate'] = f.bitrate

    if re.search("[@а-яА-Я]", track['composer']): track['composer'] = translit_to_eng(track['composer'].decode('utf-8')).encode(
        'utf-8')
    if re.search("[@а-яА-Я]", track['album']): track['album'] = translit_to_eng(track['composer'].decode('utf-8')).encode('utf-8')

    if track['composer'] == "Various": track['composer'] == "Various Artists"

    track['compilation'] = False

    # tracks = gpod.sw_get_tracks(db)
    # for trackk in tracks:
    #   if trackk.title == track.title and trackk.artist == track.artist and trackk.album == track.album and trackk.track_nr == track.track_nr:
            # _log.info('Track already in, skipping')
            # return 0

    audio = audiotools.open(path)
    metadata = audio.get_metadata()

    if metadata.images() and len(metadata.images()) > 0:
        artwork_data = metadata.images()[0].data
        if artwork_data is not None:
            track['cover'] = artwork_data
            _log.debug('storing artwork for track %s', path)

            # track['cover'] = gdk.pixbuf_new_from_stream(gio.memory_input_stream_new_from_data(artwork_data), None)

            # gpod.itdb_track_set_thumbnails_from_pixbuf(track, pixbuf)

    _log.debug('Adding track: %s (%s - %s) %s', track['albumartist'], track['year'], track['album'], track['title'])
    _log.debug('Track no.: %s/%s, CD no.: %s/%s', track['track_nr'], track['tracks'], track['cd_nr'], track['cds'])
    _log.debug('Track genre: %s', track['genre'])

    tmp = str(uuid.uuid4())
    mid_file = "/tmp/track" + tmp + ".wav"
    output_file = "/tmp/track" + tmp + ".m4a"

    if audio.lossless() or splitext(path)[1] == '.flac':
        try:
            input_file = path

            # print "files: %s and %s" % mid_file, output_file

            subprocess.call(['ffmpeg', '-y', '-loglevel', 'quiet', '-i', input_file, mid_file])
            subprocess.call(['ffmpeg', '-y', '-loglevel', 'quiet', '-i', mid_file, '-c:a', 'libfdk_aac', '-b:a', '128k', output_file])
            os.remove(mid_file)

            tr2 = audiotools.open(output_file)
            tr2.set_metadata(metadata)
        except Exception, e:
            _log.error('%s %s', e, path)
            return 0
        track['path'] = output_file
    else:
        track['path'] = path

    return track


def proc(cpu, in_queue, out_queue):
    while True:
        path = in_queue.get()
        if path is None:
            break
        print "Thread %s: Processing %s" % (cpu, path)
        tr = AddTrack(cpu, path, )
        if tr != 0: out_queue.put(tr)
        in_queue.task_done()
    print 'Worker cpu %i finished' % cpu
    in_queue.task_done()
    out_queue.put('kill')


def CopyToIpod(cpu, q, mountpoint):
    # cpu = c
    processed = 0
    _log.info('Open itunesdb')
    ipdb = gpod.itdb_parse(mountpoint, None)
    mstr = gpod.itdb_playlist_mpl(ipdb)

    while 1:
        track = q.get()
        if track == 'kill':
            cpu -= 1
            print cpu
            if cpu == 0:
                break
            else:
                continue

        tr = gpod.itdb_track_new()
        tr.filetype = track['filetype']
        tr.title = track['title']
        tr.album = track['album']
        tr.artist = track['artist']
        tr.albumartist = track['albumartist']
        tr.genre = track['genre']
        tr.composer = track['composer']
        tr.tracklen = track['tracklen']
        tr.cd_nr = track['cd_nr']
        tr.cds = track['cds']
        tr.track_nr = track['track_nr']
        tr.tracks = track['tracks']
        tr.year = track['year']
        tr.bitrate = track['bitrate']
        tr.compilation = track['compilation']
        pix = gdk.pixbuf_new_from_stream(gio.memory_input_stream_new_from_data(track['cover']), None)
        gpod.itdb_track_set_thumbnails_from_pixbuf(tr, pix)

        path = track['path']
        # print path
        gpod.itdb_track_add(ipdb, tr, -1)
        gpod.itdb_playlist_add_track(mstr, tr, -1)
        gpod.itdb_cp_track_to_ipod(tr, path, None)

        if os.path.isfile(path) and string.find(path, "tmp") > 0:
            print "Removing %s" % path
            os.remove(path)
        processed += 1
        _log.info('Processed: %i', processed)
        q.task_done()

    _log.info('Saving itunesdb')
    gpod.itdb_write(ipdb, None)
    gpod.itdb_free(ipdb)
    _log.info('Saved!')


def AddFiles(mountpoint, ipod_name, pathto):
    start = datetime.now()
    if not os.access(mountpoint, os.W_OK):
        _log.error('Ipod is not writable: %r', mountpoint)
        return

    filestoadd = []
    for pth in pathto:
        if isfile(pth):
            filename = pth
            if filename.endswith('.mp3') or filename.endswith('.m4a') or filename.endswith('.flac'):
                filestoadd.append(filename)
            continue
        for pathtoadd in glob.glob(pth):
            for root, directories, filenames in os.walk(pathtoadd):
                for filename in filenames:
                    if filename.endswith('.mp3') or filename.endswith('.m4a') or filename.endswith('.flac'):
                        filestoadd.append(os.path.join(root, filename))

    _log.info('%d items to add to %s', len(filestoadd), mountpoint)

    chunk_size = 1000
    totalfiles = len(filestoadd)
    filestoadd = [filestoadd[i:i + chunk_size] for i in range(0, len(filestoadd), chunk_size)]

    _log.debug('Adding %d chunks of %d', len(filestoadd), chunk_size)

    for filelist in filestoadd:
        nCPU = 2
        queue = multiprocessing.JoinableQueue()
        queue_out = multiprocessing.JoinableQueue()
        # queue_out = multiprocessing.Manager().JoinableQueue()

        for fl in filelist:
            queue.put(fl)
        for i in range(nCPU):
            queue.put(None)

        workers = []
        for i in range(nCPU):
            worker = multiprocessing.Process(target=proc, args=(i, queue, queue_out))
            workers.append(worker)
            worker.start()

        writProc = multiprocessing.Process(target=CopyToIpod, args=(nCPU, queue_out, mountpoint))
        writProc.start()

        queue.join()
        writProc.join()

        # print processed
        # _log.info('Processed %d/%d (%.2f%%) files in %d seconds. Average %d seconds/file', processed, totalfiles,
        #               (processed / totalfiles * 100), (datetime.now() - start).seconds,
                      # int((datetime.now() - start).seconds) / processed)

        _log.info('Processed %d/%d files in %d seconds', processed, totalfiles, (datetime.now() - start).seconds)
        _log.info('Chunk %d/%d', filestoadd.index(filelist), len(filestoadd))

    print "%d items processed, %d skipped" % (processed, skipped)
    _log.info('Done in %s', (datetime.now() - start))


def get_any_artwork(md_hard):
    try:
        return md_hard['covr'][0]
    except:
        pass

    try:
        return md_hard['APIC:'].data
    except:
        pass

    return None

# Main routine
from argparse import ArgumentParser

# logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(level=logging.INFO)

parser = ArgumentParser()

parser.add_argument(
    '-L',
    '--ipod-name',
    default='iPod',
    help="Specify name of iPod."
)

parser.add_argument(
    '-m',
    '--mountpoint',
    default='/mnt/ipod',
    help="Specify mountpoint of iPod."
)

parser.add_argument(
    '-r',
    '--recreate',
    action="store_true",
    default=False,
    help="Recreate database to wipe iPod completely"
)

parser.add_argument(
    '-a',
    '--add',
    type=str,
    nargs='+',
    default='',
    help="What to add"
)

args = parser.parse_args()

if args.recreate:
    print "Recreating"
    RecreateIpodDB(mountpoint=args.mountpoint, ipod_name=args.ipod_name)

if args.add:
    AddFiles(
        mountpoint=args.mountpoint,
        ipod_name=args.ipod_name,
        pathto=args.add,
    )
