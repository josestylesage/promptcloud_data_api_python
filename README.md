# promptcloud_data_api_python
	This is [PromptCloud's](http://promptcloud.com) data API downloader in python. It can be used to fetch the client specific data from PromptCloud data API. Available API versions are v1 and v2.

##Installing:

###Step-1
  Install python 2.7.* if not present in the system

###Step-2
   Clone this repository

##Compatibility:
	python 2.7.*

##Usage: promptcloud_data_api.py [options] 

##Options:
  -h, --help            show this help message and exit
  --apiconf=APICONF     override the config file location, the file which
                        stores information like client_id, downloadir,
                        previous timestamp file
  --promptcloudhome=PROMPTCLOUDHOME
                        to override the promptcloudhome dir:~/promptcloud
  --perform_initial_setup
                        Perform initial setup
  --display_info        Display various info
  --timestamp=TIMESTAMP
                        query promptcloudapi for files newer than or equal to
                        given timestamp
  --queried_timestamp_file=QUERIED_TIMESTAMP_FILE
                        override default queried_timestamp_file: file that
                        stores last queried timestamp
  --category=CATEGORY   query promptcloudapi for files of given category. eg:
                        if files of different verticals are placed in
                        different directory under client's parent directory,
                        then files of specific directory can be obtained by
                        specifying that directory name in category option
  --api_version=API_VERSION
                        Data Api Version as in v2/v1 [ if not added Default
                        Version is v2]
  --user=USER           Data api user id
  --pass=PASSWD         Data api password
  --client_auth_key=CLIENT_AUTH_KEY

##Example ::

###Initial Setup:
#####Run the following command
	python promptcloud_data_api.py --perform_initial_setup --user <USER_ID> --client_auth_key <CLIENT_AUTH_KEY>      # This option is for api_version v2 which is default
	         
			OR

	python promptcloud_data_api.py --perform_initial_setup --user <USER_ID> --pass <PASSWORD> --api_version v1     # This option is for api_version v1 which need to be explicitly mentioned

###Downloading Files:
#####Run the following command
	python promptcloud_data_api.py --timestamp 0    # to download all files and if a file is already downloaded it will be skipped
			
			OR
        
	python promptcloud_data_api.py --loop           # it will keep on running in the system and check the promptcloud data api if any new file has been uploaded. If any new file has been uploaded in promptcloud data api, then that file will be downloaded.


## Contributing
In order to contribute to this gem -

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new pull request
