import md5
import os
import sys
import time
import gzip
import re
import urllib2
import string
from pyInstall import installIfNeeded
installIfNeeded('yaml','pyyaml')
import yaml
installIfNeeded('requests','requests')
import requests
import optparse
installIfNeeded('httplib2','httplib2')
import httplib2
import platform
import json

class PromptCloudApi:
    if platform.system() == "Windows":
        os.environ["HOME"] = os.path.join("c:\\","Users","Public")
        
    prompt_cloud_home = os.path.join(os.environ["HOME"],"promptcloud")
    prompt_cloud_home = os.path.normpath(prompt_cloud_home)
    prompt_cloud_home_config = os.path.join(os.environ["HOME"],"promptcloudconfig.txt")
    prompt_cloud_home_config = os.path.normpath(prompt_cloud_home_config)
    
    def __init__(self,args_hash={}):
        self.api_downtime = 0
        self.download_dir = None
        self.client_id = None
        self.client_auth_key = None
        self.perform_initial_setup(args_hash)

    def display_info(self,options):
        apiconf = os.path.join(PromptCloudApi.prompt_cloud_home,"configs","config.yml")
        if options["apiconf"]:
            apiconf = options["apiconf"]

        if os.path.isfile(apiconf):
            stream_read = open(apiconf)
            conf_hash = yaml.load(stream_read)
            stream_read.close()
            for key,value in conf_hash.items():
                print "{} : {}".format(key,value)
        else:
            print >> sys.stderr, "ERROR: Config file {} doesn't exist".format(apiconf)

    
    def perform_initial_setup(self,options={}):
        file_read = open(PromptCloudApi.prompt_cloud_home_config,"ab+")
        home_info = file_read.read()
        file_read.close()
        if home_info:
            if options["promptcloudhome"] and not re.match(home_info,options["promptcloudhome"]):
                print >> sys.stderr, "ERROR: You already have a home directory at {}".format(home_info)
                sys.exit (1)
            else:
                options["promptcloudhome"] = home_info
        
        else:
            home_config_file_write = open(PromptCloudApi.prompt_cloud_home_config,"w+")
            if options["promptcloudhome"]:
                home_config_file_write.write(options["promptcloudhome"])
            
            else:
                home_config_file_write.write(PromptCloudApi.prompt_cloud_home)
                options["promptcloudhome"] = PromptCloudApi.prompt_cloud_home
            
            home_config_file_write.close()
        
        if options["promptcloudhome"]:
            PromptCloudApi.prompt_cloud_home = os.path.normpath(options["promptcloudhome"]) 

        if not os.path.isdir(PromptCloudApi.prompt_cloud_home):
            print >> sys.stderr, "Creating Download_Home directory at {}.....".format(PromptCloudApi.prompt_cloud_home)
            os.mkdir(PromptCloudApi.prompt_cloud_home)

        if not options["apiconf"]:
            options["apiconf"] = os.path.join(PromptCloudApi.prompt_cloud_home,"configs","config.yml")


        if not os.path.isdir(os.path.dirname(options["apiconf"])):
            print >> sys.stderr, "Creating APICONFIG directory  at {}.....".format(options["apiconf"])
            os.mkdir(os.path.dirname(options["apiconf"]))

        if not os.path.isfile(options["apiconf"]):
            if not options["user"]:
                print >> sys.stderr, "{} : Could not find config file : {}".format(os.path.basename(__file__),options["apiconf"])
                print >> sys.stderr, "Please enter your user id(for example if you use url http://api.promptcloud.com/data/info?id=demo then your user id is demo)\n:"
                client_id = sys.stdin.readline()
                client_id = client_id.strip(" ").rstrip("\n").strip(" ")
            else:
                client_id = options["user"]
            
            if options["api_version"] == "v2":
                if not options["client_auth_key"]:
                    print >> sys.stderr, "Please enter your auth_key\n:"
                    client_auth_key = sys.stdin.readline()
                    client_auth_key = client_id.strip(" ").rstrip("\n").strip(" ")
                else:
                    client_auth_key = options["client_auth_key"]

                yml_val = dict(client_id = client_id, 
                               download_dir = os.path.join(PromptCloudApi.prompt_cloud_home, "downloads"),
                               client_auth_key = client_auth_key)
            else:
                yml_val = dict(client_id = client_id, 
                              download_dir = os.path.join(PromptCloudApi.prompt_cloud_home, "downloads"))
            
            stream_write = open(options["apiconf"],"wb")
            yaml.dump(yml_val,stream_write,default_flow_style=False)
            stream_write.close()

        
        if not "log_dir" in options:
            options["log_dir"] = os.path.join(PromptCloudApi.prompt_cloud_home,"log")


        if not os.path.isdir(options["log_dir"]):
            print >> sys.stderr, "Creating LOG directory at {}.....".format(options["log_dir"])
            os.mkdir(options["log_dir"])

        if not os.path.isdir(os.path.join(options["log_dir"],"fetch_log")):
            os.mkdir(os.path.join(options["log_dir"],"fetch_log"))
        
        if not os.path.isdir(os.path.join(options["log_dir"],"error_log")):
            os.mkdir(os.path.join(options["log_dir"],"error_log"))
            
        if not "md5_dir" in options:
            options["md5_dir"] = os.path.join(PromptCloudApi.prompt_cloud_home,"md5sum")

        if not os.path.isdir(options["md5_dir"]):
            print >> sys.stderr, "Creating MD5SUM directory at {}.....".format(options["md5_dir"])
            os.mkdir(options["md5_dir"])

        if not options["queried_timestamp_file"] :
            options["queried_timestamp_file"] = os.path.join(PromptCloudApi.prompt_cloud_home,"last_queried_ts.txt")
        
        file_open = open(options["apiconf"],'rb')
        self.conf_hash = yaml.load(file_open)
        self.client_id = self.conf_hash["client_id"]
	if options["api_version"]  == "v2":
	    self.client_auth_key = self.conf_hash["client_auth_key"]
        file_open.close() 
        
        if not self.client_id:
            print >> sys.stderr, "{} : Could not find client_id from config file : {}".format(os.path.basename(__file__),options["apiconf"])
            sys.exit (1)
        
        self.download_dir = self.conf_hash["download_dir"]
        
        if not self.download_dir:
            self.download_dir = os.path.join(prompt_cloud_home,"downloads")
        
        if not os.path.isdir(self.download_dir):
            print >> sys.stderr, "Creating DOWNLOAD directory at {}.....".format(self.download_dir)
            os.mkdir(self.download_dir)
        print >> sys.stderr, "\nSET-UP done successfully....."
        print >> sys.stderr, "\nDOWNLOAD_HOME directory :: {}".format(PromptCloudApi.prompt_cloud_home)
        print >> sys.stderr, "\n|=====================================================================================|\n"
    
    
    def download_files(self,options):
        if options and options["api_version"] == "v1":
            if not options["user"] or not options["pass"]:
                raise Exception("You didn't provide  username and password, please provide these as hash:{user:<user_id>,pass:<password>}")
            elif options["api_version"] == "v2":
                raise Exception("You didn't provide  username and auth_key, please provide these as hash:{user:<user_id>,client_auth_key:<auth_key>}")
        
        new_feed_exists = False
        modified_time = "{:10.9f}".format(time.time())
        ts = int(str(modified_time).replace(".",""))
        fetch_log = os.path.join(options["log_dir"],"fetch_log","fetched_urls-{}.log".format(str(ts)))
        error_log = os.path.join(options["log_dir"],"error_log","error-{}.log".format(str(ts)))
        error_log_file = open(error_log,"wb")
        fetch_log_file = open(fetch_log,"wb") 
        urls_ts_map,url_md5_map = self.get_file_urls(options)
        if not url_md5_map:
            print >> sys.stderr, "{}:could not find file urls to download.".format(os.path.basename(__file__))
            error_log_file.write("\n{}:could not find file urls to download.".format(os.path.basename(__file__)))
            return new_feed_exists

        if not urls_ts_map:
            print >> sys.stderr, "No new files to download."
            return new_feed_exists
        
        sorted_ts = sorted(urls_ts_map)
        for ts in sorted_ts:
            urls = urls_ts_map[ts]
            for url in urls:
                md5sum = url_md5_map[url]
                filename = os.path.basename(url)
                md5_filename = filename.replace(".gz",".md5sum")
                md5_filepath = os.path.join(options["md5_dir"],md5_filename)
                if os.path.isfile(md5_filepath) and md5sum == open(md5_filepath,"rb").read().strip():
                    print >> sys.stderr, "Skipping file at url : {} ,it has downloaded earlier".format(url)
                    error_log_file.write("\nSkipping file at url : {} ,it has downloaded earlier".format(url))
                    continue

                new_feed_exists = True

                try:
                    print >> sys.stderr, "Fetching : {}".format(url)
                    if options["api_version"] == "v1":
                        from requests.auth import HTTPBasicAuth
                        auth = HTTPBasicAuth(options["user"],options["pass"])
                        req = requests.get(url,auth=auth)
                    else:
                        req = requests.get(url)

                    outfile = os.path.join(self.download_dir,os.path.basename(url))
                    file_name = open(outfile,"wb")
                    file_name.write(req.content)
                    fetch_log_file.write("Fetched:{} ".format(url))
                    file_name.close()

                    content = ""
                    gz = gzip.open(outfile,"rb")
                    content = gz.read()
                    
                    md5_obj = md5.new() 
                    md5_obj.update(content)
                    downloaded_md5 =md5_obj.hexdigest()
                    if md5sum == downloaded_md5:
                        file_name = open(md5_filepath,"wb")
                        file_name.write(md5sum)
                        file_name.close()
                    else:
                        print >> sys.stderr, "Url : {} was not downloaded completely, hence deleting the downloaded file".format(url)
                        error_log_file.write("\nUrl : {} was not downloaded completely, hence deleting the downloaded file".format(url))
                        outfile.delete()

                except Exception as e:
                    print >> sys.stderr, "{} : Failed to fetch url : {}".format(os.path.basename(__file__),url)
                    error_log_file.write("\n{} {} {}".format(os.path.basename(__file__),type(e),e))
                    error_log_file.write("\nFailed : {}".format(url))
                    
        fetch_log_file.close()
        error_log_file.close()
        print >> sys.stderr, "Fetch Log File :{}".format(fetch_log)
        print >> sys.stderr, "Error Log File :{}".format(fetch_log)
        print >> sys.stderr, "Downloaded files are available at:{}".format(self.download_dir)
        return new_feed_exists

    
    def get_api_url(self,options):
        if not options["api_url"]:
            api_base_url = "https://api.promptcloud.com"
            if options["bcp"]:
                api_base_url = "https://api.bcp.promptcloud.com"

            if options["api_version"] == "v1":
                promptcloud_api_query_url = api_base_url + "/data/info?id={}".format(self.client_id)
            elif options["api_version"] == "v2":
                promptcloud_api_query_url = api_base_url + "/v2/data/info?id={}&client_auth_key={}".format(self.client_id,self.client_auth_key)
            
            if options["from_date"]:
                promptcloud_api_query_url = promptcloud_api_query_url + "&from_date={}".format(options["from_date"])
        
            if options["timestamp"] or options["timestamp"] == 0:
                promptcloud_api_query_url = promptcloud_api_query_url + "&ts={}".format(options["timestamp"])
                file_name = open(options["queried_timestamp_file"],"wb+")
                file_name.write("\n{}".format(options["timestamp"]))
                file_name.close()
            elif options["timestamp"]==None and not options["from_date"]:
                if os.path.isfile(options["queried_timestamp_file"]):
                    file_name = open(options["queried_timestamp_file"],"rb")
                    last_timestamp_query = file_name.read()
                    if last_timestamp_query:
                        options["timestamp"]=int(last_timestamp_query)
                        promptcloud_api_query_url = promptcloud_api_query_url + "&ts={}".format(options["timestamp"])
                else:
                    print >> sys.stderr, "{} : Enter a timestamp from which the query is to be made. Default put --timestamp 0 to get all files uploaded till now.".format(os.path.basename(__file__))
                    print >> sys.stderr, "{} : You can enter a date in yyyymmdd format from which date all uploaded file is to be downloaded.FORMAT:: --from_date yyyymmdd.".format(os.path.basename(__file__))
                    exit(1)

            if options["category"]:
                promptcloud_api_query_url = promptcloud_api_query_url + "&cat={}".format(options["category"])
        
        else:
            promptcloud_api_query_url = options["api_url"]
        
        promptcloud_api_query_url = promptcloud_api_query_url +"&api_res_type=json"
        return promptcloud_api_query_url

    
    def handle_api_downtime(self,options):
        if self.api_downtime:
            total_downtime = Time.time()-self.api_downtime
            if total_downtime > 1800:
                options["bcp"] = True
            else:
                self.api_downtime = Time.time()
    
    
    def disable_bcp(self,options):
        if options["bcp"]:
            options["bcp"] = none
            self.api_downtime = none

    
    def get_file_urls(self,options):
        url_ts_map = {}
        url_md5_map = {}
        try:
            promptcloud_api_query = self.get_api_url(options)
            api_query_output = ""
            response = requests.get(promptcloud_api_query) 
            if response.status_code!=200:
                if options["bcp"]:
                    print >> sys.stderr, "bcp too is down !!, please mail downtime@promptcloud.com"
                    self.disable_bcp(options)
                else:
                    if options["loop"]:
                        print >> sys.stderr, "Couldn't fetch from promptcloud api server, will try after the api server after the sleep and promptcloud bcp after 30 mins"
                    else:
                        print >> sys.stderr, "Main api server seems to be unreachable, you can try --bcp options"

                    self.handle_api_downtime(options)
            else:
                api_query_output = response
                self.disable_bcp(options)

            doc = json.load(urllib2.urlopen(promptcloud_api_query))
            for entry_key in doc["root"]["entry"]:
                updated = str(entry_key["updated"])
                url = str(entry_key["url"])
                md5sum = str(entry_key["md5sum"])
                url_md5_map[url] = md5sum
                if updated in  url_ts_map:
                    url_ts_map[updated].append(url)
                else:
                    url_ts_map[updated]= [url]
            

        except Exception as e:
            print >> sys.stderr, "{} : Api query failed: {}".format(type(e),e)
            return None,None

        return url_ts_map,url_md5_map

class PromptCloudTimer:
    def __init__(self,args_hash={}):
        #super(args_hash)
        if "min" in args_hash:
            self.min = args_hash["min"]
        else:
            self.min = 10

        if "max" in args_hash:
            self.max = args_hash["max"]
        else:
            self.max = 300

        self.sleep_interval = self.min

    def wait(self):
        print >> sys.stderr, "Going to sleep for {} seconds".format(self.sleep_interval)
        time.sleep(self.sleep_interval)
        self.sleep_interval *=2
        if self.sleep_interval > 300:
            self.sleep_interval = 10

class PromptCloudApiArgParser:
    def __init__(self):
        super()
    
    @classmethod
    def validate(cls,options,mandatory):
        return_status = True
	if options["api_version"] == "v2":
            if options["perform_initial_setup"] and (not options["user"] or not options["client_auth_key"]):
                print >> sys.stderr, "{} : Please provide user_id and client_auth_key".format(os.path.basename(__file__))
                return_status = False
        elif options["api_version"] == "v1":
	    if not options["perform_initial_setup"] and not options["display_info"] and (not options["user"] or not options["pass"]):
                print >> sys.stderr, "{} : Please provide options perform_initial_setup/display_info or provide user and password for any other query".format(os.path.basename(__file__))
                return_status = False
        else:
            print >> sys.stderr, "{} : Please provide a valid version".format(os.path.basename(__file__))
            return_status = False
        return return_status

    @classmethod
    def usage_notes(cls):
        script_name = sys.argv[0]
        print >> sys.stderr, "\n Example : \n\n"

    @classmethod
    def parse(cls,defaults={},mandatory=[]):
        options = {}
        options = dict(options.items() + defaults.items())
        usage = "Usage: %prog [options] "
        from optparse import OptionParser
        opts = OptionParser(usage=usage)
        opts.add_option("--apiconf",action="store", help="override the config file location, the file which stores information like client_id, downloadir, previous timestamp file",dest="apiconf")
        opts.add_option("--promptcloudhome",action="store",help="to override the promptcloudhome dir:~/promptcloud",dest="promptcloudhome")
        opts.add_option("--perform_initial_setup",action="store_true",help="Perform initial setup",dest="perform_initial_setup")
        opts.add_option("--display_info",action="store_true",help="Display various info",dest="display_info")
        opts.add_option("--timestamp",action="store",type="int",help="query promptcloudapi for files newer than or equal to given timestamp",dest="timestamp")
        opts.add_option("--queried_timestamp_file",action="store",help="override default queried_timestamp_file: file that stores last queried timestamp",dest="queried_timestamp_file",default=None)
        opts.add_option("--category",action="store",help="query promptcloudapi for files of given category. eg: if files of different verticals are placed in different directory under client's parent directory, then files of specific directory can be obtained by specifying that directory name in category option",dest="category")
	opts.add_option("--api_version",action="store",help="Data Api Version as in v2/v1 [ if not added Default Version is v2]",dest="api_version",default="v2")
        opts.add_option("--user",action="store",help="Data api user id",dest="user")
        opts.add_option("--pass",action="store",help="Data api password",dest="passwd") 
        opts.add_option("--client_auth_key",action="store",help="to add AUTHKEY provided to client",dest="client_auth_key")
	opts.add_option("--loop",action="store_true",help="download new data files and keep looking for new one. i.e it doesn't exit, if no new feed is found it will sleep. minimun sleep time is 10 secs and max sleep time is 300 secs",dest="loop")
        opts.add_option("--site_name",action="store",help="Download files for a particular site as in xyz_com for xyz.com",dest="site_name")
        opts.add_option("--noloop",action="store_true",help="Download new data files and and exit, this is the default behaviour",dest="noloop")
        opts.add_option("--api_url",action="store",help="If a api-url of the form \"https://api.promptcloud.com/data/info?id=client_id&cat=cat_1&from_date=yyyymmdd\" is provided just pass it as the option",dest="api_url")
        opts.add_option("--from_date",action="store",help = "Mention date in yyyymmdd format to get updated files from this date",dest="from_date")
        opts.add_option("--bcp",action="store_true",help="use bcp.promptcloud.com instead of api.promptcloud.com",dest="bcp")
        args_hash,args = opts.parse_args()
        options["apiconf"]=args_hash.apiconf
        options["promptcloudhome"] = args_hash.promptcloudhome
        options["perform_initial_setup"] = args_hash.perform_initial_setup
        options["api_version"] = args_hash.api_version
        options["display_info"] = args_hash.display_info
        options["timestamp"] = args_hash.timestamp
        options["queried_timestamp_file"] = args_hash.queried_timestamp_file
        options["category"] = args_hash.category
        options["user"] = args_hash.user
        options["pass"] = args_hash.passwd
        options["client_auth_key"] = args_hash.client_auth_key
        options["loop"] = args_hash.loop
        options["noloop"] = args_hash.noloop
        options["bcp"] = args_hash.bcp
        options["site_name"] = args_hash.site_name
        options["api_url"] = args_hash.api_url
        options["from_date"] = args_hash.from_date
        
        if cls.validate(options,mandatory):
            return options
            
        else:
            print >> sys.stderr, "{} Invalid/no args, use -h command for help".format(os.path.basename(__file__))
            sys.exit(-1)


def main():
    show_banner()
    options= PromptCloudApiArgParser.parse()
    obj = PromptCloudApi(options)
    timer = PromptCloudTimer()
    if options["display_info"]:
        obj.display_info(options)
    elif options["perform_initial_setup"]:
       obj.perform_initial_setup(options)
    else:
        if options["loop"]:
            while (1):
                new_feed_exists = obj.download_files(options)
                if new_feed_exists == False:
                    timer.wait()
        else:
            obj.download_files(options)

def show_banner():
    print >> sys.stderr, "|=====================================================================================|"
    print >> sys.stderr, "|                        PROMPTCLOUD API DOWNLOADER                                   |"
    print >> sys.stderr, "|=====================================================================================|"
    print >> sys.stderr, "\n"

if __name__ == '__main__':
    main()


