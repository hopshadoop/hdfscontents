"""A contents manager that uses HDFS file system for storage."""

# Copyright (c) A
# Distributed under the terms of the Modified BSD License.

from hdfs3 import HDFileSystem
from hdfscontents.hdfsio import HDFSManagerMixin
from hdfscontents.hdfscheckpoints import HDFSCheckpoints
from notebook.services.contents.manager import ContentsManager
from notebook.utils import to_os_path
try:  # new notebook
    from notebook import _tz as tz
except ImportError: # old notebook
    from notebook.services.contents import tz
from tornado import web
from tornado.web import HTTPError
import mimetypes
import nbformat
from traitlets import Instance, Integer, Unicode, default

try:  # PY3
    from base64 import encodebytes, decodebytes
except ImportError:  # PY2
    from base64 import encodestring as encodebytes, decodestring as decodebytes


class HDFSContentsManager(ContentsManager, HDFSManagerMixin):
    """
    ContentsManager that persists to HDFS filesystem local filesystem.
    """

    hdfs_namenode_host = Unicode(u'localhost', config=True, help='The HDFS namenode host')
    hdfs_namenode_port = Integer(9000, config=True, help='The HDFS namenode port')
    hdfs_user = Unicode(None, allow_none=True, config=True, help='The HDFS user name')

    root_dir = Unicode(u'/', config=True, help='The HDFS root directory to use')

    # The HDFS3 object used to interact with HDFS cluster.
    hdfs = Instance(HDFileSystem, config=True)

    @default('hdfs')
    def _default_hdfs(self):
        return HDFileSystem(host=self.hdfs_namenode_host, port=self.hdfs_namenode_port, user=self.hdfs_user)

    def _checkpoints_class_default(self):
        # TODO: a better way to pass hdfs and root_dir?
        HDFSCheckpoints.hdfs = self.hdfs
        HDFSCheckpoints.root_dir = self.root_dir
        return HDFSCheckpoints

    # ContentsManager API part 1: methods that must be
    # implemented in subclasses.

    def dir_exists(self, path):
        """Does a directory exist at the given path?
        Like os.path.isdir
        Parameters
        ----------
        path : string
            The relative API style path to check
        Returns
        -------
        exists : bool
            Whether the path does indeed exist.
        """
        path = path.strip('/')
        hdfs_path = to_os_path(path, self.root_dir)

        return self._hdfs_dir_exists(hdfs_path)

    def is_hidden(self, path):
        """Is path a hidden directory or file?
        Parameters
        ----------
        path : string
            The path to check. This is an API path (`/` separated,
            relative to root dir).
        Returns
        -------
        hidden : bool
            Whether the path is hidden.
        """
        path = path.strip('/')
        hdfs_path = to_os_path(path, self.root_dir)
        return self._hdfs_is_hidden(hdfs_path)


    def file_exists(self, path=''):
        """Does a file exist at the given path?
        Like os.path.isfile
        Override this method in subclasses.
        Parameters
        ----------
        path : string
            The API path of a file to check for.
        Returns
        -------
        exists : bool
            Whether the file exists.
        """
        path = path.strip('/')
        hdfs_path = to_os_path(path, self.root_dir)

        return self._hdfs_file_exists(hdfs_path)

    def exists(self, path):
        """Does a file or directory exist at the given path?
        Like os.path.exists
        Parameters
        ----------
        path : string
            The API path of a file or directory to check for.
        Returns
        -------
        exists : bool
            Whether the target exists.
        """

        path = path.strip('/')
        hdfs_path = to_os_path(path, self.root_dir)

        return self.hdfs.exists(hdfs_path)

    def _base_model(self, path):
        """Build the common base of a hdfscontents model"""
        hdfs_path = to_os_path(path, self.root_dir)

        info = self.hdfs.info(hdfs_path)
        last_modified = tz.utcfromtimestamp(info.get(u'last_mod'))

        # TODO: don't have time created! now storing last accessed instead
        created = tz.utcfromtimestamp(info.get(u'last_access'))
        # Create the base model.
        model = {}
        model['name'] = path.rsplit('/', 1)[-1]
        model['path'] = path
        model['last_modified'] = last_modified
        model['created'] = created
        model['content'] = None
        model['format'] = None
        model['mimetype'] = None

        # TODO: Now just checking if user have write permission in HDFS. Need to cover all cases and check the user & group
        try:
            model['writable'] = (info.get(u'permissions') & 0o0200) > 0
        except OSError:
            self.log.error("Failed to check write permissions on %s", hdfs_path)
            model['writable'] = False
        return model

    def _dir_model(self, path, content=True):
        """Build a model for a directory
        if content is requested, will include a listing of the directory
        """
        hdfs_path = to_os_path(path, self.root_dir)
        four_o_four = u'directory does not exist: %r' % path

        if not self.dir_exists(path):
            raise web.HTTPError(404, four_o_four)
        elif self.is_hidden(path):
            self.log.info("Refusing to serve hidden directory %r, via 404 Error",
                          hdfs_path
                          )
            raise web.HTTPError(404, four_o_four)

        model = self._base_model(path)
        model['type'] = 'directory'
        if content:
            model['content'] = contents = []

            for subpath in self.hdfs.ls(hdfs_path, detail=False):

                name = subpath.strip('/').rsplit('/', 1)[-1]
                if self.should_list(name) and not self._hdfs_is_hidden(subpath):
                    contents.append(self.get(
                        path='%s/%s' % (path, name),
                        content=False)
                    )

            model['format'] = 'json'
        return model

    def _file_model(self, path, content=True, format=None):
        """Build a model for a file
        if content is requested, include the file hdfscontents.
        format:
          If 'text', the hdfscontents will be decoded as UTF-8.
          If 'base64', the raw bytes hdfscontents will be encoded as base64.
          If not specified, try to decode as UTF-8, and fall back to base64
        """
        model = self._base_model(path)
        model['type'] = 'file'

        hdfs_path = to_os_path(path, self.root_dir)
        model['mimetype'] = mimetypes.guess_type(hdfs_path)[0]

        if content:
            content, format = self._read_file(hdfs_path, format)
            if model['mimetype'] is None:
                default_mime = {
                    'text': 'text/plain',
                    'base64': 'application/octet-stream'
                }[format]
                model['mimetype'] = default_mime

            model.update(
                content=content,
                format=format,
            )

        return model

    def _notebook_model(self, path, content=True):
        """Build a notebook model
        if content is requested, the notebook content will be populated
        as a JSON structure (not double-serialized)
        """
        model = self._base_model(path)
        model['type'] = 'notebook'
        if content:
            hdfs_path = to_os_path(path, self.root_dir)
            nb = self._read_notebook(hdfs_path, as_version=4)
            self.mark_trusted_cells(nb, path)
            model['content'] = nb
            model['format'] = 'json'
            self.validate_notebook_model(model)
        return model

    def _save_directory(self, hdfs_path, model, path=''):
        """create a directory"""
        if self._hdfs_is_hidden(hdfs_path):
            raise HTTPError(400, u'Cannot create hidden directory %r' % hdfs_path)
        if not self.hdfs.exists(hdfs_path):
            try:
                self.hdfs.mkdir(hdfs_path)
            except:
                raise HTTPError(403, u'Permission denied: %s' % path)
        elif not self._hdfs_dir_exists(hdfs_path):
            raise HTTPError(400, u'Not a directory: %s' % (hdfs_path))
        else:
            self.log.debug("Directory %r already exists", hdfs_path)

    def get(self, path, content=True, type=None, format=None):
        """Get a file or directory model."""
        """ Takes a path for an entity and returns its model
                Parameters
                ----------
                path : str
                    the API path that describes the relative path for the target
                content : bool
                    Whether to include the hdfscontents in the reply
                type : str, optional
                    The requested type - 'file', 'notebook', or 'directory'.
                    Will raise HTTPError 400 if the content doesn't match.
                format : str, optional
                    The requested format for file contents. 'text' or 'base64'.
                    Ignored if this returns a notebook or directory model.
                Returns
                -------
                model : dict
                    the contents model. If content=True, returns the contents
                    of the file or directory as well.
                """
        path = path.strip('/')

        if not self.exists(path):
            raise web.HTTPError(404, u'No such file or directory: %s' % path)

        if self.dir_exists(path):
            if type not in (None, 'directory'):
                raise web.HTTPError(400,
                                    u'%s is a directory, not a %s' % (path, type), reason='bad type')
            model = self._dir_model(path, content=content)
        elif type == 'notebook' or (type is None and path.endswith('.ipynb')):
            model = self._notebook_model(path, content=content)
        else:
            if type == 'directory':
                raise web.HTTPError(400,
                                    u'%s is not a directory' % path, reason='bad type')
            model = self._file_model(path, content=content, format=format)
        return model

    def save(self, model, path=''):
            """
                    Save a file or directory model to path.
                    Should return the saved model with no content.  Save implementations
                    should call self.run_pre_save_hook(model=model, path=path) prior to
                    writing any data.
                    """
            path = path.strip('/')

            if 'type' not in model:
                raise web.HTTPError(400, u'No file type provided')
            if 'content' not in model and model['type'] != 'directory':
                raise web.HTTPError(400, u'No file content provided')

            path = path.strip('/')
            hdfs_path = to_os_path(path, self.root_dir)
            self.log.debug("Saving %s", hdfs_path)

            self.run_pre_save_hook(model=model, path=path)

            try:
                if model['type'] == 'notebook':
                    nb = nbformat.from_dict(model['content'])
                    self.check_and_sign(nb, path)
                    self._save_notebook(hdfs_path, nb)
                    # One checkpoint should always exist for notebooks.
                    if not self.checkpoints.list_checkpoints(path):
                        self.create_checkpoint(path)
                elif model['type'] == 'file':
                    # Missing format will be handled internally by _save_file.
                    self._save_file(hdfs_path, model['content'], model.get('format'))
                elif model['type'] == 'directory':
                    self._save_directory(hdfs_path, model, path)
                else:
                    raise web.HTTPError(400, "Unhandled hdfscontents type: %s" % model['type'])
            except web.HTTPError:
                raise
            except Exception as e:
                self.log.error(u'Error while saving file: %s %s', path, e, exc_info=True)
                raise web.HTTPError(500, u'Unexpected error while saving file: %s %s' % (path, e))

            validation_message = None
            if model['type'] == 'notebook':
                self.validate_notebook_model(model)
                validation_message = model.get('message', None)

            model = self.get(path, content=False)
            if validation_message:
                model['message'] = validation_message

            #self.run_post_save_hook(model=model, os_path=hdfs_path)

            return model

    def delete_file(self, path):
        """Delete file at path."""
        path = path.strip('/')
        hdfs_path = to_os_path(path, self.root_dir)
        if self._hdfs_dir_exists(hdfs_path):

            listing = self.hdfs.ls(hdfs_path, detail=False)
            # Don't delete non-empty directories.
            # A directory containing only leftover checkpoints is
            # considered empty.
            cp_dir = getattr(self.checkpoints, 'checkpoint_dir', None)
            for longentry in listing:
                entry = longentry.strip('/').rsplit('/', 1)[-1]
                if entry != cp_dir:
                    raise web.HTTPError(400, u'Directory %s not empty' % hdfs_path)
        elif not self._hdfs_file_exists(hdfs_path):
            raise web.HTTPError(404, u'File does not exist: %s' % hdfs_path)

        if self._hdfs_dir_exists(hdfs_path):
            self.log.debug("Removing directory %s", hdfs_path)
            try:
                self.hdfs.rm(hdfs_path, recursive=True)
            except:
                raise HTTPError(403, u'Permission denied: %s' % path)
        else:
            self.log.debug("Removing file %s", hdfs_path)
            try:
                self.hdfs.rm(hdfs_path, recursive=False)
            except:
                raise HTTPError(403, u'Permission denied: %s' % path)

    def rename_file(self, old_path, new_path):
        """Rename a file."""
        old_path = old_path.strip('/')
        new_path = new_path.strip('/')
        if new_path == old_path:
            return

        new_hdfs_path = to_os_path(new_path, self.root_dir)
        old_hdfs_path = to_os_path(old_path, self.root_dir)

        # Should we proceed with the move?
        if self.hdfs.exists(new_hdfs_path):
            raise web.HTTPError(409, u'File already exists: %s' % new_path)

        # Move the file
        try:
            self._hdfs_move_file(old_hdfs_path, new_hdfs_path)
        except Exception as e:
            raise web.HTTPError(500, u'Unknown error renaming file: %s %s' % (old_path, e))

    def info_string(self):
        return "Serving notebooks from HDFS directory: %s" % self.root_dir
