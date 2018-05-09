import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager, Link
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
from urllib import urlopen
import re, os
from time import time
from collections import defaultdict
try:
    # For python 2
    from urlparse import urlparse, parse_qs, urljoin
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = (set() 
    if not os.path.exists("successful_urls.txt") else 
    set([line.strip() for line in open("successful_urls.txt").readlines() if line.strip() != ""]))
MAX_LINKS_TO_DOWNLOAD = 3000

current_count = len(url_count)
subdomains = defaultdict(int)
invalid = 0

most_links = ""
most_links_count = 0


@Producer(ProducedLink, Link)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "64849059_93059720_47543037"
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "IR S17 UnderGrad 64849059, 47543037, 93059720"
		
        self.frame = frame
        assert(self.UserAgentString != None)
        assert(self.app_id != "")
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu", self.UserAgentString)
        print l.full_url
        self.frame.add(l)

    def update(self):
        global invalid
        for g in self.frame.get_new(OneUnProcessedGroup):
            print "Got a Group"
            outputLinks, urlResps = process_url_group(g, self.UserAgentString)
            for urlResp in urlResps:
                if urlResp.bad_url and self.UserAgentString not in set(urlResp.dataframe_obj.bad_url):
                    urlResp.dataframe_obj.bad_url += [self.UserAgentString]
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
                if not is_valid(l):
                    invalid += 1
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def shutdown(self):
        links = len(url_count)
        endtime = time() - self.starttime
        print "downloaded ", links, " in ", endtime, " seconds."
        write_report(links, endtime)


def save_count(urls):
    global url_count
    urls = set(urls).difference(url_count)
    url_count.update(urls)
    if len(urls):
        with open("successful_urls.txt", "a") as surls:
            surls.write(("\n".join(urls) + "\n").encode("utf-8"))

def process_url_group(group, useragentstr):
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    save_count(successfull_urls)
    return extract_next_links(rawDatas), rawDatas
    
#######################################################################################
'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''
def write_report(links, time):
    global subdomains
    global invalid
    global most_links
    global most_links_count
    global current_count
    i = 1
    while os.path.exists("report{}.txt".format(i)):
        i += 1
    f = open("report{}.txt".format(i), "w")
    for d, c in subdomains.items():
        f.write("{} ---- {}\n".format(d, c))
    f.write("{}: {}\n".format("\nInvalid Links", invalid))
    f.write("\nFile with most outgoing links: \n")
    f.write("{} ---- {}\n".format(most_links, most_links_count))

    f.write("\n\nDownloaded {} in {} seconds\n".format(links - current_count, time))
    f.write("Total URLS: {}\n".format(links))
    f.close()

def extract_next_links(rawDatas):
    global subdomains
    global most_links
    global most_links_count
    outputLinks = list()
    '''
    rawDatas is a list of objs -> [raw_content_obj1, raw_content_obj2, ....]
    Each obj is of type UrlResponse  declared at L28-42 datamodel/search/datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.

    Suggested library: lxml
    '''
    for link in rawDatas:
        base_url = link.url
        if link.is_redirected:
            base_url = link.final_url

        if base_url == None or base_url.count("/") > 30:
            link.bad_url = True
            continue    
        if base_url != None and base_url != "":
            try:
                
                parsed = urlparse(base_url)
                tree = html.parse(base_url).getroot()
                if tree == None:
                    link.bad_url = True
                    continue
                rel_urls = tree.xpath('//a/@href')
                # print("######"+base_url+"######")


                unique_urls = set()
                count = 0
                for rel in rel_urls:
                    if rel == None or " " in rel or "#" == rel or "mailto:" in rel or len(rel) < 3:
                        continue

                    if "http" in rel:
                        new_url = rel
                    elif rel[0] == "/":
                        new_url = parsed.scheme + "://" + parsed.netloc + rel
                    elif rel[0:3] == "../":
                        new_url = urljoin(parsed.scheme + "://" + parsed.netloc + parsed.path + 
                                            ("/" if base_url[-1] != "/" else ""), rel)
                    else:
                        new_url = base_url + ("/" if base_url[-1] != "/" else "") + rel
                    # print("Old: " + rel)
                    # print("New: " + new_url + "\n")
                    unique_urls.update(new_url)
                    count += 1
                    outputLinks.append(new_url)

                unique_count = len(unique_urls)
                subdomains[parsed.hostname] += unique_count
                if count > most_links_count:
                    most_links = base_url
                    most_links_count = count
            except IOError:
                link.bad_url = True

                
    return outputLinks
def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''
    if url != None:
        if len(url) >= 1000 or url.count("/") > 30:
            return False
        invalid = ["../", "~/", "?", " "]
        if any(i in url for i in invalid):
            return False

    parsed = urlparse(url)

    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        if "calendar" in parsed.hostname: #Calendar trap
            return False

        if parsed.path.count(".") > 1: #If there is any . in the path besides extension.
            return False
                 
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
