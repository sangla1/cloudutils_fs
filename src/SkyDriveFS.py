from skydrive import api_v5
import six
import os
import time
from UserDict import UserDict

# python filesystem imports
from fs.base import FS
from fs.errors import PathError, UnsupportedError, \
                      CreateFailedError, ResourceInvalidError, \
                      ResourceNotFoundError, NoPathURLError, \
                      OperationFailedError
from fs.remote import RemoteFileBuffer
from fs.filelike import SpooledTemporaryFile

# Items in cache are considered expired after 5 minutes.
CACHE_TTL = 300
# Max size for spooling to memory before using disk (5M).
MAX_BUFFER = 1024**2*5

class CacheItem(object):
    """Represents a path in the cache. There are two components to a path.
       It's individual metadata, and the children contained within it."""
    def __init__(self, metadata=None, children=None, timestamp=None):
        self.metadata = metadata
        self.children = children
        if timestamp is None:
            timestamp = time.time()
        self.timestamp = timestamp

    def add_child(self, name):
        if self.children is None:
            self.children = [name]
        else:
            self.children.append(name)

    def del_child(self, name):
        if self.children is None:
            return
        try:
            i = self.children.index(name)
        except ValueError:
            return
        self.children.pop(i)

    def _get_expired(self):
        if self.timestamp <= time.time() - CACHE_TTL:
            return True
    expired = property(_get_expired)

    def renew(self):
        self.timestamp = time.time()

class SkyDriveCache(UserDict):
    def set(self, path, metadata):
        self[path] = CacheItem(metadata)

    def pop(self, path, default=None):
        value = UserDict.pop(self, path, default)
        return value


class SkyDriveClient(api_v5.SkyDriveAPI):
    def __init__(self, access_token):
        self.auth_access_token = access_token
        self.cache = SkyDriveCache()
        
    def metadata(self, path):
        "Gets metadata for a given path."
        item = self.cache.get(path)
        if not item or item.metadata is None or item.expired:
            try:
                metadata = super(SkyDriveClient, self).info(path)
            except api_v5.ProtocolError, e:
                if e.code == 404:
                    raise ResourceNotFoundError(path)
                
                raise OperationFailedError(opname='metadata', path=path,
                                            msg=str(e) )
            
            item = self.cache[path] = CacheItem(metadata)
        # Copy the info so the caller cannot affect our cache.
        return dict(item.metadata.items())
    
    def children(self, path):
        "Gets children of a given path."
        update = False
        item = self.cache.get(path)
        if item:
            if item.expired:
                update = True
            else:
                if item.metadata["type"] != "folder" and not ("folder" in path):
                    raise ResourceInvalidError(path)
            if not item.children:
                update = True
        else:
            update = True
        if update:
            try:
                metadata = super(SkyDriveClient, self).info(path)
                if metadata["type"] != "folder" and not ("folder" in path):
                    raise ResourceInvalidError(path)
                children = []
                contents = super(SkyDriveClient, self).listdir(path)
                for child in contents:
                    children.append(child['id'])
                    self.cache[child['id']] = CacheItem(child)
                item = self.cache[path] = CacheItem(metadata, children)
            except api_v5.ProtocolError, e:
                if e.code == 404:
                    raise ResourceNotFoundError(path)
                if not item or e.resp.status != 304:
                    raise OperationFailedError(opname='metadata',path=path, msg=str(e) )
                # We have an item from cache (perhaps expired), but it's
                # hash is still valid (as far as SkyDrive is concerned),
                # so just renew it and keep using it.
                item.renew()
        return item.children
    
    def file_create_folder(self, parent_id, title):
        "Add newly created directory to cache."
        try:
            metadata = super(SkyDriveClient, self).mkdir(title, parent_id)
        except api_v5.ProtocolError, e:
            if e.code == 405:
                    raise ResourceInvalidError(parent_id)
            if e.code == 404:
                    raise ResourceNotFoundError(parent_id)
            raise OperationFailedError(opname='file_create_folder', msg=str(e) )
            
        self.cache.set(metadata["id"], metadata)
        return metadata
    
    def file_copy(self, src, dst):
        try:
            metadata = super(SkyDriveClient, self).copy(src, dst, False)
        except api_v5.ProtocolError, e:
            if e.code == 404:
                raise ResourceNotFoundError("Parent or source file don't exist")
            raise OperationFailedError(opname='file_copy', msg= str(e) )
            
        self.cache.set(dst, metadata)
        
    def file_move(self, src, dst):
        try:
            metadata = super(SkyDriveClient, self).copy(src, dst, True)
        except api_v5.ProtocolError, e:
            if e.code == 404:
                raise ResourceNotFoundError("Parent or source file don't exist")
            raise OperationFailedError(opname='file_copy', msg= str(e) )
            
        self.cache.set(dst, metadata)
            
        self.cache.pop(src, None)
        self.cache.set(dst, metadata)
    
    def file_delete(self, path):
        try:
            super(SkyDriveClient, self).delete(path)
        except api_v5.ProtocolError, e:
            if e.code == 404:
                raise ResourceNotFoundError(path)
            raise OperationFailedError(opname='file_copy', msg=str(e) )
        self.cache.pop(path, None)
    
    def put_file(self, parent_id, title, content, overwrite=False):
        try:
            metadata = super(SkyDriveClient, self).put((title, content), parent_id, overwrite=overwrite)
        except api_v5.ProtocolError, e:
            if e.code == 404:
                raise ResourceNotFoundError(parent_id)
            raise OperationFailedError(opname='file_copy', msg=str(e) )
        except TypeError, e:
            raise ResourceInvalidError("put_file")
        
        self.cache.set(metadata['id'], metadata)
        
        
    def get_file(self, file_id):
        metadata = None
        
        if( not self.cache.get(file_id, None) ):
            try: 
                metadata = super(SkyDriveClient, self).info(file_id)
            except api_v5.ProtocolError, e:
                if e.code == 404:
                    raise ResourceNotFoundError("Source file doesn't exist")
                
                raise OperationFailedError(opname='get_file', msg=str(e) )
            
            self.cache.set(metadata['id'], metadata)
        else:
            item = self.cache[file_id]
            metadata = item.metadata
            
        return super(SkyDriveClient, self).get(file_id) 
    
    def update_file(self, file_id, new_file_info):
        try: 
            metadata = super(SkyDriveClient, self).info_update(file_id, new_file_info)
        except api_v5.ProtocolError, e:
            if e.resp.status == 404:
                raise ResourceNotFoundError(path=file_id)
            
            raise OperationFailedError(opname='file_copy', msg=e.resp.reason )
         
        self.cache.pop(file_id, None)
        self.cache.set(metadata['id'], metadata)
        
        
        
class SkyDriveFS(FS):
    """
        Sky drive file system
    """
    
    _meta = { 'thread_safe' : True,
              'virtual': False,
              'read_only' : False,
              'unicode_paths' : True,
              'case_insensitive_paths' : False,
              'network' : True,
              'atomic.move' : True,
              'atomic.copy' : True,
              'atomic.makedir' : True,
              'atomic.rename' : False,
              'atomic.setconetns' : True
              }

    def __init__(self, root=None, credentials=None, thread_synchronize=True, caching=False, 
                 scope=["wl.skydrive_update"]):
        self._root = root
        self._credentials = credentials
        self.cached_files = {}
        self._cacheing = caching
        self._skydrive = api_v5.SkyDriveAPI()
        
        
        
        if (self._root == None):
            self._root = "me/skydrive"
        
        
        if( self._credentials == None ):
            if( "SKYDRIVE_ACCESS_TOKEN" not in os.environ ):
                raise CreateFailedError("SKYDRIVE_ACCESS_TOKEN is not set in os.environ")
            else:
                self._credentials['access_token'] = os.environ.get('DROPBOX_ACCESS_TOKEN')
        
        self.client = SkyDriveClient(self._credentials["access_token"]) 
        super(SkyDriveFS, self).__init__(thread_synchronize=thread_synchronize)

        
    
    def __repr__(self):
        args = (self.__class__.__name__, self._root)
        
        return '< FileSystem: %s - Root Directory: %s >' % args

    __str__ = __repr__
    
    
    def _update(self, path, data):
        """
        Updates content of an existing file
        
        @param path: Id of the file for which to update content
        @param data: content to write to the file  
        """
        path = self._normpath(path)
        
        if isinstance(data, basestring):
            string_data = data
        else:
            try:
                data.seek(0)
                string_data = data.read()
            except:
                raise ResourceInvalidError("Unsupported type")
            
        f = self.getinfo(path)
        return self.client.put( (f["name"], string_data), f["parent_id"], True )

    
    def setcontents(self, path, data="", chunk_size=64*1024, **kwargs):
        """
        Sets new content to remote file
        
        Method works only with existing files and sets new content to them.
        @param path: Id of the file in which to write the new content
        @param data: File content as a string, or a StringIO object
        @param kwargs: additional parameters like:
            encoding: the type of encoding to use if data is text
            errors: encoding errors
        @param chunk_size: Number of bytes to read in a chunk, if the implementation has to resort to a read / 
            copy loop 
        """
        path = self._normpath(path)
         
        encoding = kwargs.get("encoding", None)
        errors = kwargs.get("errors", None)
        
        if isinstance(data, six.text_type):
            data = data.encode(encoding=encoding, errors=errors)
        
        
        self._update(path, data)

    
    
    def createfile(self, path, wipe=True, **kwargs):
        """Creates an empty file always, even if another file with the same name exists
        
        @param path: path to the new file. It has to be in one of following forms:
            - parent_id/file_title.ext
            - file_title.ext or /file_title.ext - In this cases root directory is the parent
        @param wipe: New file with empty content. In the case of google drive it will
            always be True
        @param kwargs: Additional parameters like: 
            description - a short description of the new file 
        @raise PathError: If parent doesn't exist
        
        """
        parts = path.split("/")
        if(parts[0] == ""):
            parent_id = self._root
            title = parts[1]
        elif( len(parts) == 2):
            parent_id = parts[0]
            title = parts[1]
            if( not self.exists(parent_id) ):
                raise PathError("parent doesn't exist")
        else:
            parent_id = self._root
            title = parts[0]

        
        self.client.put((title, ""), parent_id, True)
        
    def open(self, path, mode='r',  buffering=-1, encoding=None, 
             errors=None, newline=None, line_buffering=False, **kwargs):
        """Open the named file in the given mode.

        This method downloads the file contents into a local temporary file
        so that it can be worked on efficiently.  Any changes made to the
        file are only sent back to cloud storage when the file is flushed or closed.
        """
        path = self._normpath(path)
        
        spooled_file = SpooledTemporaryFile(mode=mode, bufsize=MAX_BUFFER)
        
        #  Truncate the file if requested
        if "w" in mode:
            self._update(path, "")
        else:
            try:
                spooled_file.write( self.client.get_file( path ) )
                spooled_file.seek(0, 0)
            except Exception, e:
                if "w" not in mode and "a" not in mode:
                    raise ResourceNotFoundError("%r" % e)
                else:
                    self.createfile(path, True)

        
        return RemoteFileBuffer(self,path,mode,spooled_file)
   
        
    def is_root(self, path):
        path = self._normpath(path)
        if( path == self._root):
            return True
        else:
            return False
    def rename(self, src, dst):
        """
        @param src: id of the file to be renamed 
        @param dst: new title of the file
        """
        if self.is_root(path = src):
            raise UnsupportedError("Can't rename the root directory")  
        
        
        return self.client.update_file(src, {"name": dst})
  
    def remove(self, path):
        """
        @param path: id of the folder to be deleted
        @return: None if removal was successful 
        """
        path = self._normpath(path)
        if self.is_root(path = path):
            raise UnsupportedError("Can't remove the root directory")   
        if self.isdir(path = path):
            raise PathError("Specified path is a directory")  

        return self.client.file_delete(path)
    
    def removedir(self, path):
        """
        @param path: id of the folder to be deleted
        @return: None if removal was successful 
        """     
        path = self._normpath(path)
           
        if not self.isdir(path):
            raise PathError("Specified path is a directory") 
        if self.is_root(path = path):
            raise UnsupportedError("remove the root directory")
        
        return self.client.file_delete(path)
    
    def makedir(self, path, recursive=False, allow_recreate=False ):
        """
        @param path: path to the folder you want to create.
            it has to be in one of the following forms:
                - parent_id/new_folder_name  (when recursive is False)
                - parent_id/new_folder1/new_folder2...  (when recursive is True)
                - /new_folder_name to create a new folder in root directory
                - /new_folder1/new_folder2... to recursively create a new folder in root
        @param recursive: allows recursive creation of directories
        @param allow_recreate: for google drive this param is always False, it will
            never recreate a directory with the same id ( same names are allowed )
        """
        parts = path.split("/")
        
        if( parts[0] == "" ):
            parent_id = self._root
        elif( len(parts) >= 2 ):
            parent_id = parts[0]
            if( not self.exists(parent_id) ):
                raise PathError("parent with the id '%s' doesn't exist" % parent_id)
            
        if( len(parts) > 2):
            if( recursive ):
                for i in range( len(parts) - 1 ):
                    title = parts[i+1]
                    resp = self.client.file_create_folder(parent_id, title) 
                    parent_id=resp["id"]
            else:
                raise UnsupportedError("recursively create a folder")
        else:
            if( len(parts) == 1 ):
                title = parts[0]
                parent_id = self._root
            else:
                title = parts[1]
            return self.client.file_create_folder(parent_id, title) 
        
    def move(self, src, dst, overwrite=False, chunk_size=16384):
        """
        @param src: id of the file to be moved
        @param dst: id of the folder in which the file will be moved
        @param overwrite: for SkyDrive it is always false
        @param chunk_size: if using chunk upload
        
        @note: folder can't be moved, this is a limitation of skydrive API 
        """
                
        if( self.isdir(src) ):
            raise UnsupportedError("move a directory")
        
        
        self.client.file_move(src, dst)
    
    def movedir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=16384):
        """
        @attention: skydrive API doesn't allow to move folders
        """
        raise UnsupportedError("move a directory")   
    
    def isdir(self, path):
        """
        Checks if the given path is a folder
        
        @param path: id of the object to check
        @attention: this method doesn't check if the given path exists
            it will return true or false even if the file/folder doesn't exist
        """
        
        
        path = self._normpath(path)
        
        info = self.getinfo(path)
        return "folder" in path or (info['type']=="folder")
    
    def isfile(self, path):
        """
        Checks if the given path is a file
        
        @param path: id of the object to check 
        @attention: this method doesn't check if the given path exists
            it will return true or false even if the file/folder doesn't exist
        """
        path = self._normpath(path)
        info = self.getinfo(path)
        return "file" in path or (info['type']=="file")
    
    
    def exists(self, path):
        try:
            return self.client.metadata(path)
        except:
            return False

    
    def listdir(self, path=None,
                      wildcard=None,
                      full=False,
                      absolute=False,
                      dirs_only=False,
                      files_only=False,
                      overrideCache=False
                      ):
        path = self._normpath(path)
        flist = self.client.children(path)
        dirContent = self._listdir_helper('', flist, wildcard, full, absolute, dirs_only, files_only)
        
        return dirContent
    
    #Optimised listdir from pyfs
    def listdirinfo(self, path=None,
                          wildcard=None,
                          full=False,
                          absolute=False,
                          dirs_only=False,
                          files_only=False):
        
        path = self._normpath(path)
        

        return [(p, self.getinfo(p))
                    for p in self.listdir(path,
                                          wildcard=wildcard,
                                          full=full,
                                          absolute=absolute,
                                          dirs_only=dirs_only,
                                          files_only=files_only)]


    def getinfo(self, path):
        """
        @param path: file id for which to return informations
        @return: dictionary with informations about the specific file 
        @raise PathError: if the provided path doesn't exist 
        """
        path = self._normpath(path)
        if(not self.exists(path)):
            raise PathError("Specified path doesn't exist")
        
        resp = self.client.metadata(path)
        return resp
        
    
    def getpathurl(self, path, allow_none=False):
        """Returns a url that corresponds to the given path, if one exists.
        
        If the path does not have an equivalent URL form (and allow_none is False)
        then a :class:`~fs.errors.NoPathURLError` exception is thrown. Otherwise the URL will be
        returns as an unicode string.
        
        @param path: object id for which to return url path
        @param allow_none: if true, this method can return None if there is no
            URL form of the given path
        @type allow_none: bool
        @raises `fs.errors.NoPathURLError`: If no URL form exists, and allow_none is False (the default)
        @rtype: unicode 
        
        """
        path = self._normpath(path)
        url = None
        try:
            url = self.getinfo(path)['source']
        except:
            if not allow_none:
                raise NoPathURLError(path=path)

        return url
    
    def _normpath(self, path):
        #TODO: Well known folders are a problem
        
        if(path == self._root):
            return path
        elif(path == None):
            return self._root
        elif( len( path.split("/") ) > 2 ):
            return path.split("/")[-1]
        elif(path[0] == "/" and len(path) == 1):
            return self._root
        elif(path[0] == "/"):
            return path[1:]
        elif(len(path) == 0):
            return self._root
        
        return path
"""
Problems:
  - Flush and close, both call write contents and because of that 
    the file on cloud is overwrite twice...
"""

