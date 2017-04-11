"""
HDFS-based Checkpoints implementations.
"""
import os
from hdfscontents.hdfsio import HDFSManagerMixin
from tornado.web import HTTPError
from notebook.services.contents.checkpoints import Checkpoints

from traitlets import Unicode
try:  # new notebook
    from notebook import _tz as tz
except ImportError: # old notebook
    from notebook.services.contents import tz


class HDFSCheckpoints(HDFSManagerMixin, Checkpoints):
    """
    A Checkpoints that caches checkpoints for files in adjacent
    directories.
    Only works with FileContentsManager.  Use GenericFileCheckpoints if
    you want file-based checkpoints with another ContentsManager.
    """

    checkpoint_dir = Unicode(
        u'.ipynb_checkpoints',
        config=True,
        help="""The directory name in which to keep file checkpoints
        This is a path relative to the file's own directory.
        By default, it is .ipynb_checkpoints
        """,
    )

    hdfs = None
    root_dire = None

    # ContentsManager-dependent checkpoint API
    def create_checkpoint(self, contents_mgr, path):
        """Create a checkpoint."""
        checkpoint_id = u'checkpoint'
        src_path = contents_mgr._get_hdfs_path(path)
        dest_path = self.checkpoint_path(checkpoint_id, path)
        self._copy(src_path, dest_path)
        return self.checkpoint_model(checkpoint_id, dest_path)

    def restore_checkpoint(self, contents_mgr, checkpoint_id, path):
        """Restore a checkpoint."""
        src_path = self.checkpoint_path(checkpoint_id, path)
        dest_path = contents_mgr._get_hdfs_path(path)
        self._copy(src_path, dest_path)

    # ContentsManager-independent checkpoint API
    def rename_checkpoint(self, checkpoint_id, old_path, new_path):
        """Rename a checkpoint from old_path to new_path."""
        old_cp_path = self.checkpoint_path(checkpoint_id, old_path)
        new_cp_path = self.checkpoint_path(checkpoint_id, new_path)
        if self._hdfs_file_exists(old_cp_path):
            self.log.debug(
                "Renaming checkpoint %s -> %s",
                old_cp_path,
                new_cp_path,
            )
            with self.perm_to_403():
                self._hdfs_move_file(old_cp_path, new_cp_path)

    def delete_checkpoint(self, checkpoint_id, path):
        """delete a file's checkpoint"""
        path = path.strip('/')
        cp_path = self.checkpoint_path(checkpoint_id, path)
        if not self._hdfs_file_exists(cp_path):
            self.no_such_checkpoint(path, checkpoint_id)

        self.log.debug("unlinking %s", cp_path)
        with self.perm_to_403():
            self.hdfs.rm(cp_path)

    def list_checkpoints(self, path):
        """list the checkpoints for a given file
        This contents manager currently only supports one checkpoint per file.
        """
        path = path.strip('/')
        checkpoint_id = "checkpoint"
        cp_path = self.checkpoint_path(checkpoint_id, path)
        if not self._hdfs_file_exists(cp_path):
            return []
        else:
            return [self.checkpoint_model(checkpoint_id, cp_path)]

    # Checkpoint-related utilities
    def checkpoint_path(self, checkpoint_id, path):
        """find the path to a checkpoint"""
        path = path.strip('/')
        parent, name = ('/' + path).rsplit('/', 1)
        parent = parent.strip('/')
        basename, ext = os.path.splitext(name)
        filename = u"{name}-{checkpoint_id}{ext}".format(
            name=basename,
            checkpoint_id=checkpoint_id,
            ext=ext,
        )
        hdfs_path = self._get_hdfs_path(path=parent)
        cp_dir = os.path.join(hdfs_path, self.checkpoint_dir)
        with self.perm_to_403():
            self._hdfs_ensure_dir_exists(cp_dir)
        cp_path = os.path.join(cp_dir, filename)
        return cp_path

    def checkpoint_model(self, checkpoint_id, hdfs_path):
        """construct the info dict for a given checkpoint"""
        stats = self.hdfs.info(hdfs_path)
        last_modified = tz.utcfromtimestamp(stats.get(u'last_mod'))

        info = dict(
            id=checkpoint_id,
            last_modified=last_modified,
        )
        return info

    # Error Handling
    def no_such_checkpoint(self, path, checkpoint_id):
        raise HTTPError(
            404,
            u'Checkpoint does not exist: %s@%s' % (path, checkpoint_id)
        )

