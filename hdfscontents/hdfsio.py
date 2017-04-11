"""
Utilities for file-based Contents/Checkpoints managers.
"""

# Copyright (c)
# Distributed under the terms of the Modified BSD License.

from contextlib import contextmanager
import errno
import os
from tornado.web import HTTPError
from notebook.utils import (
    to_api_path,
    to_os_path,
)
import nbformat
from ipython_genutils.py3compat import str_to_unicode
from traitlets.config import Configurable
from traitlets import Bool, Integer, Unicode, default, Instance
try:  # PY3
    from base64 import encodebytes, decodebytes
except ImportError:  # PY2
    from base64 import encodestring as encodebytes, decodestring as decodebytes


def path_to_intermediate(path):
    """Name of the intermediate file used in atomic writes.
    The .~ prefix will make Dropbox ignore the temporary file."""
    dirname, basename = os.path.split(path)
    return os.path.join(dirname, '.~' + basename)


def path_to_invalid(path):
    """Name of invalid file after a failed atomic write and subsequent read."""
    dirname, basename = os.path.split(path)
    return os.path.join(dirname, basename + '.invalid')


def hdfs_copy_file(hdfs, src, dst):
        chunk = 2 ** 16
        # TODO: check if we need to specify replication
        with hdfs.open(dst, 'wb') as f1:
            with hdfs.open(src, 'rb') as f2:
                while True:
                    out = f2.read(chunk)
                    if len(out) == 0:
                        break
                    f1.write(out)
        f1.close()
        f1.flush()
        f2.close()


def hdfs_replace_file(hdfs, src, dst):
    """ replace dst with src
    switches between os.replace or os.rename based on python 2.7 or python 3
    """
    hdfs.rm(dst)
    hdfs.mv(src, dst)


def hdfs_file_exists(hdfs, hdfs_path):
    return hdfs.exists(hdfs_path) and hdfs.info(hdfs_path).get(u'kind') == u'file'

@contextmanager
def atomic_writing(hdfs, hdfs_path):
    """Context manager to write to a file only if the entire write is successful.
    This works by copying the previous file contents to a temporary file in the
    same directory, and renaming that file back to the target if the context
    exits with an error. If the context is successful, the new data is synced to
    disk and the temporary file is removed.
    Parameters
    ----------
    hdfs : HDFileSystem
      the hdfs3 object
    hdfs_path : str
      The target file to write to.
    """

    tmp_path = path_to_intermediate(hdfs_path)

    if hdfs_file_exists(hdfs, hdfs_path):
        hdfs_copy_file(hdfs, hdfs_path, tmp_path)

    fileobj = hdfs.open(hdfs_path, 'wb')

    try:
        yield fileobj
    except:
        # Failed! Move the backup file back to the real path to avoid corruption
        fileobj.close()
        hdfs_replace_file(hdfs, tmp_path, hdfs_path)
        raise

    # Flush to disk
    fileobj.flush()
    fileobj.close()

    # Written successfully, now remove the backup copy
    if hdfs_file_exists(hdfs, tmp_path):
        hdfs.rm(tmp_path)


@contextmanager
def _simple_writing(hdfs, hdfs_path):
    """Context manager to write to a file only if the entire write is successful.
    This works by copying the previous file hdfscontents to a temporary file in the
    same directory, and renaming that file back to the target if the context
    exits with an error. If the context is successful, the new data is synced to
    disk and the temporary file is removed.
    Parameters
    ----------
    hdfs : HDFileSystem
      the hdfs3 object
    hdfs_path : str
      The target file to write to.
    """

    # Text mode is not supported in HDFS3
    fileobj = hdfs.open(hdfs_path, 'wb')

    try:
        yield fileobj
    except:
        #   # Failed! Move the backup file back to the real path to avoid corruption
        fileobj.close()
        raise

    # Flush to disk
    fileobj.flush()
    fileobj.close()


class HDFSManagerMixin(Configurable):
    """
    Mixin for ContentsAPI classes that interact with the HDFS filesystem.
    Provides facilities for reading, writing, and copying both notebooks and
    generic files.
    Shared by HDFSContentsManager and HDFSCheckpoints.
    Note
    ----
    Classes using this mixin must provide the following attributes:
    root_dir : unicode
        A directory against against which API-style paths are to be resolved.
    hdfs : HDFileSystem
        To communicate with the HDFS cluster
    log : logging.Logger
    """

    use_atomic_writing = Bool(True, config=True, help=
    """By default notebooks are saved on disk on a temporary file and then if succefully written, it replaces the old ones.
      This procedure, namely 'atomic_writing', causes some bugs on file system whitout operation order enforcement (like some networked fs).
      If set to False, the new notebook is written directly on the old one which could fail (eg: full filesystem or quota )""")

    def _hdfs_dir_exists(self, hdfs_path):
        """Does the directory exists in HDFS filesystem?
        Parameters
        ----------
        hdfs_path : string
            The absolute HDFS path to check
        Returns
        -------
        exists : bool
            Whether the path does indeed exist.
        """

        if self.hdfs.exists(hdfs_path):
            return self.hdfs.info(hdfs_path).get(u'kind') == u'directory'
        else:
            return False

    def _hdfs_ensure_dir_exists(self, hdfs_path):
        """ensure that a directory exists

        If it doesn't exist, try to create it and protect against a race condition
        if another process is doing the same.

        """
        if not self.hdfs.exists(hdfs_path):
            try:
                self.hdfs.mkdir(hdfs_path)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
        elif not self._hdfs_dir_exists(hdfs_path):
            raise IOError("%r exists but is not a directory" % hdfs_path)

    def _hdfs_is_hidden(self, hdfs_path):
        """Is path in HDFS hidden directory or file?
        checks if any part in the path starts with a dot '.'
        Parameters
        ----------
        hdfs_path : string
            The path to check. This is an absolute HDFS path).
        Returns
        -------
        hidden : bool
            Whether the path is hidden.
        """

        if any(part.startswith('.') for part in hdfs_path.split('/')):
            return True
        return False

    def _hdfs_file_exists(self, hdfs_path):
        """Does a file exist at the given path?
        Like os.path.isfile

        Parameters
        ----------
        hdfs_path : string
            The absolute HDFS path of a file to check for.
        Returns
        -------
        exists : bool
            Whether the file exists.
        """

        if (self.hdfs.exists(hdfs_path)):
            return self.hdfs.info(hdfs_path).get(u'kind') == u'file'
        else:
            return False

    def _hdfs_move_file(self, src, dst):
        if self._hdfs_file_exists(dst):
            self.hdfs.rm(dst)
        self.hdfs.mv(src, dst)

    def _hdfs_copy_file(self, src, dst):
        hdfs_copy_file(self.hdfs, src, dst)

    @contextmanager
    def atomic_writing(self, hdfs_path):
        """wrapper around atomic_writing that turns permission errors to 403.
        Depending on flag 'use_atomic_writing', the wrapper perform an actual atomic writing or
        simply writes the file (whatever an old exists or not)"""
        with self.perm_to_403(hdfs_path):
            if self.use_atomic_writing:
                with atomic_writing(self.hdfs, hdfs_path) as f:
                    yield f
            else:
                with _simple_writing(self.hdfs, hdfs_path) as f:
                    yield f

    @contextmanager
    def perm_to_403(self, hdfs_path=''):
        """context manager for turning permission errors into 403."""
        try:
            yield
        except (OSError, IOError) as e:
            if e.errno in {errno.EPERM, errno.EACCES}:
                # make 403 error message without root prefix
                # this may not work perfectly on unicode paths on Python 2,
                # but nobody should be doing that anyway.
                if not hdfs_path:
                    hdfs_path = str_to_unicode(e.filename or 'unknown file')
                path = to_api_path(hdfs_path, root=self.root_dir)
                raise HTTPError(403, u'Permission denied: %s' % path)
            else:
                raise

    def _copy(self, src, dst):
        """copy src to dest
        """
        self._hdfs_copy_file(src, dst)

    def _get_hdfs_path(self, path):

        return to_os_path(path, self.root_dir)

    def _read_notebook(self, hdfs_path, as_version=4):
        """Read a notebook from an os path."""
        # TODO: check for open errors
        with self.hdfs.open(hdfs_path, 'rb') as f:
            try:
                return nbformat.read(f, as_version=as_version)
            except Exception as e:
                e_orig = e

            # If use_atomic_writing is enabled, we'll guess that it was also
            # enabled when this notebook was written and look for a valid
            # atomic intermediate.
            tmp_path = path_to_intermediate(hdfs_path)

            if not self.use_atomic_writing or not self.hdfs.exists(tmp_path):
                raise HTTPError(
                    400,
                    u"Unreadable Notebook: %s %r" % (hdfs_path, e_orig),
                )

            # Move the bad file aside, restore the intermediate, and try again.
            invalid_file = path_to_invalid(hdfs_path)
            self._hdfs_move_file(hdfs_path, invalid_file)
            self._hdfs_move_file(tmp_path, hdfs_path)
            return self._read_notebook(hdfs_path, as_version)

    def _save_notebook(self, hdfs_path, nb):
        """Save a notebook to an os_path."""
        with self.atomic_writing(hdfs_path) as f:
            nbformat.write(nb, f, version=nbformat.NO_CONVERT)

    def _read_file(self, hdfs_path, format):
        """Read a non-notebook file.
        os_path: The path to be read.
        format:
          If 'text', the hdfscontents will be decoded as UTF-8.
          If 'base64', the raw bytes hdfscontents will be encoded as base64.
          If not specified, try to decode as UTF-8, and fall back to base64
        """
        if not self._hdfs_file_exists(hdfs_path):
            raise HTTPError(400, "Cannot read non-file %s" % hdfs_path)

        with self.hdfs.open(hdfs_path, 'rb') as f:
            bcontent = f.read()

        if format is None or format == 'text':
            # Try to interpret as unicode if format is unknown or if unicode
            # was explicitly requested.
            try:
                return bcontent.decode('utf8'), 'text'
            except UnicodeError:
                if format == 'text':
                    raise HTTPError(
                        400,
                        "%s is not UTF-8 encoded" % hdfs_path,
                        reason='bad format',
                    )
        return encodebytes(bcontent).decode('ascii'), 'base64'

    def _save_file(self, hdfs_path, content, format):
        """Save content of a generic file."""
        if format not in {'text', 'base64'}:
            raise HTTPError(
                400,
                "Must specify format of file hdfscontents as 'text' or 'base64'",
            )
        try:
            if format == 'text':
                bcontent = content.encode('utf8')
            else:
                b64_bytes = content.encode('ascii')
                bcontent = decodebytes(b64_bytes)
        except Exception as e:
            raise HTTPError(
                400, u'Encoding error saving %s: %s' % (hdfs_path, e)
            )

        with self.atomic_writing(hdfs_path) as f:
            f.write(bcontent)
