from DropboxFS import DropboxFS
from GoogleDriveFS import GoogleDriveFS
from SkyDriveFS import SkyDriveFS
import dropbox
from cloudutils_config import *
from dropbox.client import DropboxOAuth2Flow  

class CloudServiceFactory(object):
    def get_fs(self, uri, user=None, callback_url=None, request = None):
        service_name = uri.split("://")[0]
        root = uri.split("://")[1]
        cloudutils_settings = user.get('cloudutils_settings', {})
        credentials = cloudutils_settings.get(service_name, {})
        
        filesystem = None
        if(service_name == 'dropbox'):
            filesystem = self._build_dropbox_fs(user, credentials, root, callback_url, request)
        elif(service_name == 'google_drive'):
            filesystem = GoogleDriveFS(None, credentials)
        elif(service_name == 'sky_drive'):
            filesystem = SkyDriveFS(None, credentials)
        
        return filesystem
        
    def _build_dropbox_fs(self, user, credentials, root=None, callback_url=None, request=None):
        if(request == None ):
            try:
                filesystem = DropboxFS(root, credentials)
                filesystem.about()
                return filesystem
            except:
                #Remove everything from user credentials
                #Session nije ovo
                self.session={}
                flow = dropbox.client.DropboxOAuth2Flow(
                                                        CFG_DROPBOX_KEY, 
                                                        CFG_DROPBOX_SECRET, 
                                                        callback_url, self.session, 
                                                        CFG_DROPBOX_CSRF_TOKEN
                                                        )
                
                url = flow.start()
                return url
          
        elif(request != None):
            try:
                access_token, uid, url_state = dropbox.client.DropboxOAuth2Flow(
                    CFG_DROPBOX_KEY, 
                    CFG_DROPBOX_SECRET, callback_url, self.session, 
                    CFG_DROPBOX_CSRF_TOKEN
                    ).finish( request )
            except Exception, e:
                return None
    
            newSettings = {
                            'dropbox': {
                                    'uid': uid,
                                    'access_token': access_token
                                    }
                           }
            #self.update_cloudutils_settings(newSettings)
            
            filesystem = DropboxFS(root, {"access_token": access_token})
            return filesystem
    
    
    
    
"""    
    def update_cloudutils_settings(self, newData):
        # Updates cloudutils settings in DataBase and refreshes current user
        user = User.query.get(current_user.get_id())
        settings = user.settings
        cloudutils_settings = settings.get("cloudutils_settings")
        
        if( cloudutils_settings ):
            cloudutils_settings.update( newData )
            
            #TODO: Check why is this necessary
            settings.update(settings)
        else:
            settings.update({"cloudutils_settings" : newData})
        
        user.settings = settings
        db.session.merge(user)
        db.session.commit()
        current_user.reload()
        
        """