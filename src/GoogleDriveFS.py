import six
from StringIO import StringIO
import mimetypes
import os
import datetime

# python filesystem imports
from fs.base import FS
from fs.errors import PathError, UnsupportedError, \
                      CreateFailedError, ResourceInvalidError, \
                      ResourceNotFoundError, NoPathURLError
from fs.remote import RemoteFileBuffer
from fs.filelike import LimitBytesFile

# Imports specific to google drive service
import httplib2
from apiclient.discovery import build
from apiclient.http import MediaInMemoryUpload
from oauth2client.client import OAuth2Credentials



class GoogleDriveFS(FS):
    """
        Google drive file system
        
        
        
        @attention: when setting variables in os.environ please note that 
            GD_TOKEN_EXPIRY has to be in format: "%Y, %m, %d, %H, %M, %S, %f"
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

    def __init__(self, root=None, credentials=None, thread_synchronize=True, caching=False):
        self._root = root
        self._credentials = credentials
        self.cached_files = {}
        self._cacheing = caching
        
        def _getDateTimeFromString(time):
            if time:
                return datetime.datetime.strptime( time, "%Y, %m, %d, %H, %M, %S, %f" )
            else:
                return None            
        
        if( self._credentials == None ):
            if( "GD_ACCESS_TOKEN" not in os.environ or
                "GD_CLIENT_ID" not in os.environ or
                "GD_CLIENT_SECRET" not in os.environ or
                "GD_TOKEN_EXPIRY" not in os.environ or
                "GD_TOKEN_URI" not in os.environ):
                raise CreateFailedError("You need to set:\n" + \
                                         "GD_ACCESS_TOKEN, GD_CLIENT_ID, GD_CLIENT_SECRET" + \
                                         " GD_TOKEN_EXPIRY, GD_TOKEN_URI in os.environ")
            else:
                credentials = OAuth2Credentials(
                                os.environ.get('GD_ACCESS_TOKEN'), 
                                os.environ.get('GD_CLIENT_ID'), 
                                os.environ.get('GD_CLIENT_SECRET'), 
                                None, 
                                _getDateTimeFromString( os.environ.get('GD_TOKEN_EXPIRY') ),
                                os.environ.get('GD_TOKEN_URI'), 
                                None
                                )
        
                
        if (self._root == None):
            service = self._build_service(self._credentials)
            about = service.about().get().execute()
            self._root = about.get("rootFolderId")
            
        super(GoogleDriveFS, self).__init__(thread_synchronize=thread_synchronize)

        
    
    def __repr__(self):
        args = (self.__class__.__name__, self._root)
        
        return 'FileSystem: %s \nRoot: %s' % args

    __str__ = __repr__
    
    
    def _update(self, path, data):
        if isinstance(data, basestring):
            string_data = data
        else:
            try:
                data.seek(0)
                string_data = data.read()
            except:
                raise ResourceInvalidError("Unsupported type")
            
        
        return self._cloud_command("update_file", path=path, content=string_data)
    
    def setcontents(self, path, data="", encoding=None, errors=None, chunk_size=64*1024):
        """
        Sets contents to remote file.
        @param path: Id of the file
        @param data: File content as a string, or a StringIO object
        """ 
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
        
        if(kwargs.has_key("description")):
            description = kwargs['description']    
        else:
            description = ""
        
        self._cloud_command("create_new_file", title=title, parent_id=parent_id, description=description)
        
    def open(self, path, mode='r',  buffering=-1, encoding=None, 
             errors=None, newline=None, line_buffering=False, **kwargs):
        """Open the named file in the given mode.

        This method downloads the file contents into a local temporary file
        so that it can be worked on efficiently.  Any changes made to the
        file are only sent back to cloud storage when the file is flushed or closed.
        """
        
        file_content = StringIO()
        
        if self.isdir(path):
            raise ResourceInvalidError("'%s' is a directory" % path)
        
        
        #  Truncate the file if requested
        if "w" in mode:
            self._update(path, "")
        else:
            try:
                file_content.write( self._cloud_command("get_file", path=path ) )
                file_content.seek(0, 0)
            except Exception, e:
                if "w" not in mode and "a" not in mode:
                    raise ResourceNotFoundError("%r" % e)
                else:
                    self._upload(path, "")
        
        f = LimitBytesFile(file_content.len, file_content, "r")
            
       
        #  For streaming reads, return the key object directly
        if mode == "r-":
            return f
        
        #  For everything else, use a RemoteFileBuffer.
        #  This will take care of closing the socket when it's done.
        return RemoteFileBuffer(self,path,mode,f)
   
        
    def is_root(self, path):
        if( path == self._root):
            return True
        else:
            return False
        
        
    
    def copy(self, src, dst, overwrite=False, chunk_size=1024 * 64):
        """
        @param src: Id of the file to be copied
        @param dst: Id of the folder in which to copy the file
        """
        if( self.isdir(src) ):
            raise PathError("Specified src is a directory. Please use copydir.")
        
        
        self._copy(src, dst)
    
    def copydir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=16384):
        """
        @attention: Google drive doesn't support copy of folders. And to implement it 
            over copy method will be very inefficient 
        """
        
        raise NotImplemented("If implemented method will be very inefficient")
    
    
    def _copy(self, src, dst):
        src_info = self.exists(src)
        if( not src_info ):
            raise PathError("Specified src doesn't exist")
        
        if( not self.isdir(dst) ):
            raise PathError("Specified dst is not a folder")
        
        return self._cloud_command("copy_file", src=src, dst=dst)
    
    def rename(self, src, dst):
        """
        @param src: id of the file to be renamed 
        @param dst: new title of the file
        """
        if self.is_root(path = src):
            raise UnsupportedError("Can't rename the root directory")  
        
        resp = self._cloud_command('file_rename', file_id=src, title=dst)
        return resp
  
    def remove(self, path):
        """
        @param path: id of the folder to be deleted
        @return: None if removal was successful 
        """
        
        if self.is_root(path = path):
            raise UnsupportedError("Can't remove the root directory")   
        if self.isdir(path = path):
            raise PathError("Specified path is a directory. Please use removedir.")  

        return self._cloud_command('file_delete', path=path)
    
    def removedir(self, path):
        """
        @param path: id of the folder to be deleted
        @return: None if removal was successful 
        """        
        if not self.isdir(path):
            raise PathError("Specified path is a directory") 
        if self.is_root(path = path):
            raise UnsupportedError("remove the root directory")
        
        return self._cloud_command('file_delete', path=path)
    
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
                    resp = self._cloud_command("file_create_folder", parent_id=parent_id, title=title)
                    parent_id=resp["id"]
            else:
                raise UnsupportedError("recursively create a folder")
        else:
            if( len(parts) == 1 ):
                title = parts[0]
                parent_id = self._root
            else:
                title = parts[1]
            self._cloud_command("file_create_folder", parent_id=parent_id, title=title)
    
    def move(self, src, dst, overwrite=False, chunk_size=16384):
        """
        @param src: id of the file to be moved
        @param dst: id of the folder in which the file will be moved
        @param overwrite: for Google drive it is always false
        @param chunk_size: if using chunk upload
        
        @note: google drive can have many parents for one file, when using this 
            method a file will be moved from all current parents to the new 
            parent 'dst'
        """ 
        if( self.isdir(src) ):
            raise PathError("Specified src is a directory. Please use movedir.")
        self._move(src, dst)
    
    def movedir(self, src, dst, overwrite=False, ignore_errors=False, chunk_size=16384):
        """
        @param src: id of the folder to be moved
        @param dst: id of the folder in which the file will be moved
        @param overwrite: for Google drive it is always false
        @param chunk_size: if using chunk upload
        
        @note: google drive can have many parents for one folder, when using this 
            method a folder will be moved from all current parents to the new 
            parent 'dst'
        """  
        if( self.isfile(src) ):
            raise PathError("Specified src is a file. Please use move.")
        self._move(src, dst)
    
    def _move(self, src, dst):
        src_info = self.exists(src)
        dst_info = self.exists(dst)       
             
        if( not ( src_info or dst_info ) ):
            raise PathError("Source or destination don't exist")
        if( self._isfile(dst_info) ):
            raise PathError("Specified destination is not a folder")

        src_info['parents'] = [{"id": dst}]
        self._cloud_command("update_file_info", path=src, new_file=src_info)
        
    def _isdir(self, info):
        return info["mimeType"] == "application/vnd.google-apps.folder"
        
    def isdir(self, path):
        if( path == "/" ):
            path = self._root
            return True
        try:
            info = self.getinfo(path)
        except:           
            raise PathError(path)
        
        return self._isdir(info)

    
    def _isfile(self, info):
        return info["mimeType"] != "application/vnd.google-apps.folder"
    
    def isfile(self, path):
        try:
            info = self.getinfo(path)
        except:           
            raise PathError(path)
        
        self._isfile(info)
    
    
    def exists(self, path):
        try:
            return self._cloud_command("get_file_info", path = path)
        except:
            return False
    
    
    def _get_dir_list_from_service(self, metadata):
        flist = []
        if metadata and metadata.has_key('items'):
            for one in metadata['items']:
                flist.append(one['id'])
                
        return flist 
    
    def listdir(self, path=None,
                      wildcard=None,
                      full=False,
                      absolute=False,
                      dirs_only=False,
                      files_only=False,
                      overrideCache=False
                      ):
        if( not path or path == "/" ):
            path = self._root
        data = self._cloud_command('list_dir', path=path )
        flist = self._get_dir_list_from_service( data )

        dirContent = self._listdir_helper('', flist, wildcard, full, absolute, dirs_only, files_only)
        return dirContent
    

    def listdirinfo(self, path=None,
                          wildcard=None,
                          full=False,
                          absolute=False,
                          dirs_only=False,
                          files_only=False):
        
        if( not path ):
            path = self._root
        
        metadata = self._cloud_command('list_dir_with_info', path=path)
        
        def getinfo(p):
            if( metadata.has_key('items') ):
                contents = metadata['items']
                for one in contents:
                    if( one['id'] == p ):
                        return one
             
            return {}   

        return [(p, getinfo(p))
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
        if(not self.exists(path)):
            raise PathError("Specified path doesn't exist")
        
        resp = self._cloud_command("get_file_info", path = path)
        return resp
        
    
    def getpathurl(self, path, allow_none=False):
        """Returns a url that corresponds to the given path, if one exists.
        
        If the path does not have an equivalent URL form (and allow_none is False)
        then a :class:`~fs.errors.NoPathURLError` exception is thrown. Otherwise the URL will be
        returns as an unicode string.
        
        @param path: id of the file for which to return the url path
        @param allow_none: if true, this method can return None if there is no
            URL form of the given path
        @type allow_none: bool
        @raises `fs.errors.NoPathURLError`: If no URL form exists, and allow_none is False (the default)
        @rtype: unicode 
        
        """
        
        url = None
        try:
            url = self.getinfo(path)
            url = url["webContentLink"]
        except:
            if not allow_none:
                raise NoPathURLError(path=path)

        return url
    
    
    def _build_service(self, credentials):
        http = httplib2.Http()
        http = credentials.authorize(http);
        service = build('drive', 'v2', http=http)
        return service
        
    def _cloud_command(self, cmd, **kwargs):
        path = kwargs.get('path', self._root)
        service = self._build_service(self._credentials)
        
        # This is needed for browse, and some parts of pyfs
        # because it always puts / on the begining
        if( path[0] == "/" ):
            path = path[1:]
        if( len(path) == 0 ):
            path = self._root
        
        
        if cmd == 'list_dir':
            # Return directory list
            resp = service.children().list(folderId=path).execute()
            return resp
        elif cmd == 'list_dir_with_info':
            # Return directory list
            param = {"q":  "'%s' in parents" % path}
            resp = service.files().list(**param).execute()
            return resp
        elif cmd == 'get_file':
            if( not self.cached_files.has_key(path) ): 
                f = service.files().get(fileId=path).execute()
                if(self._cacheing):
                    self.cached_files[f["id"]] = f
            else:
                f = self.cached_files[path]
                
            download_url = f.get('downloadUrl')
            resp, content = service._http.request(download_url)
            if( resp.status == 200 ):
                return content
            else:
                return None   
        elif cmd == 'get_file_info':
            f = service.files().get(fileId=path).execute()
            return f  
        elif cmd == 'create_new_file':
            title = kwargs.get("title", "untitled.txt")
            parent_id = kwargs.get("parent_id", self._root)
            description = kwargs.get("description", "")
            body = {
                    "title": title,
                    "parents": [{"id": parent_id}],
                    "description": description,
                    "mimeType": mimetypes.guess_type(title)
                    }
            f = service.files().insert(
                                       body = body
                                       ).execute()

            return f 
        elif cmd == 'file_create_folder':
            # Creates a new empty directory
            parent_id = kwargs.get("parent_id", self._root)
            title = kwargs.get("title", "untitled")
            body = {
                    "title": title,
                    "parents": [{"id": parent_id}],
                    "mimeType": "application/vnd.google-apps.folder"
                    }
            resp = service.files().insert(body=body).execute()
            return resp
        elif cmd == 'file_delete':
            # Deletes file
            resp = service.files().delete(fileId=path).execute()
            # Return empty body if everything was OK
            return resp
        elif cmd == 'copy_file':
            source_id = kwargs.get('src','')
            parent_id = kwargs.get('dst','') 
            
            body = {"parents": [{"id": parent_id}]}
            
            return service.files().copy(
                                        fileId = source_id,
                                        body = body
                                        ).execute()
            
        elif cmd == 'file_rename':
            file_id = kwargs.get('file_id','')
            title = kwargs.get('title','untitled') 
            f = self.cached_files.get(path, None)

            if( f == None ):
                f = service.files().get(fileId=file_id).execute()
            
            f['title'] = title    
            updated_file = service.files().update( fileId = file_id,
                                                   body = f
                                                  ).execute()
            if(self._cacheing):                                      
                self.cached_files[path] = updated_file
            return updated_file        
        elif cmd == 'update_file':
            # Updates a file on google drive
            f = self.cached_files.get(path, None)
            if( f == None ):
                f = service.files().get(fileId=path).execute()
            content = kwargs.get("content")
            media_body = MediaInMemoryUpload(content)
            updated_file = service.files().update(
                                                  fileId = path,
                                                  body = f,
                                                  media_body=media_body
                                                  ).execute()
            if(self._cacheing):
                self.cached_files[path] = updated_file
            return updated_file
        elif cmd == 'update_file_info':
            # Updates a file on google drive
            f = kwargs.get("new_file")
            updated_file = service.files().patch(
                                                  fileId = path,
                                                  body = f,
                                                  fields="parents"
                                                  ).execute()
            if(self._cacheing):
                self.cached_files[path] = updated_file
            return updated_file
        return None
    
    
    
"""
Problems:
  - Flush and close, both call write contents and because of that 
    the file on cloud is overwrite twice...
"""