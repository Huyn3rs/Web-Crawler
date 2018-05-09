default:
	python applications/search/crawler.py -a amazon.ics.uci.edu -p 9050

test:
	python applications/search/crawler.py -a amazon.ics.uci.edu -p 9100
	 
