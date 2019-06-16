import requests
from concurrent.futures import ThreadPoolExecutor
import json
import multiprocessing
import csv

THREADS = 10
PAGE_SIZE = 200
BRAND_REFINEMENTS_FILE = 'brand_refinements.json'
token = 'Bearer v^1.1#i^1#p^1#I^3#f^0#r^0#t^H4sIAAAAAAAAAOVYa2wUVRTubrctyCsxvCwE1kETlczsnZl9zUjXbF+yPNrCllqaSrk7c4eO3ZlZ5s7arkZpKgH1h0Qg4hPr449GEDBEQDQYkT8SDSQYFQQbY4JBAoUfRhOjd2a3ZVsJULpIE+fPZM4999zvO+c7984M6C4d/8CGhRt+n+Qqc/d2g263y8VOAONLS+ZPLnaXlxSBPAdXb/c93Z6e4rMLMNSSKXE5wilDx8jbpSV1LDrGCipt6qIBsYpFHWoIi5YkxqNLl4gcA8SUaViGZCQpb6y6gvJLMpAFXoCCIiNZkYhVH4jZaFRQnCz4A5xMbnwoBASFjGOcRjEdW1C3yDhgBRoEaTbYyIZFEBB5gQnzfAvlbUImVg2duDCAijhwRWeumYf12lAhxsi0SBAqEovWxuujseqausYFvrxYkVwe4ha00njoU5UhI28TTKbRtZfBjrcYT0sSwpjyRbIrDA0qRgfA3AR8J9UBgYMBHglBVoESRKGCpLLWMDVoXRuHbVFlWnFcRaRbqpW5XkZJNhKPIcnKPdWRELFqr31bloZJVVGRWUHVVEZXRhsaqMgKM95OSMh0TQJmaiFupxuWV9NBv6ywcgCGaZjgpHBCArmFstFyaR62UpWhy6qdNOytM6xKRFCjobkJiYG83BCner3ejCqWjSjPjwMDOeRCLXZRs1VMW+26XVekkUR4ncfrV2BwtmWZaiJtocEIwwecFFVQMJVSZWr4oKPFnHy6cAXVblkp0efr7OxkOnnGMNf4OABYX/PSJXGpHWmQIr52r2f91etPoFWHioTITKyKViZFsHQRrRIA+hoqwgt+AYRyeR8KKzLc+i9DHmff0I4oVIeEgoqCgglO4KEfBlAhGiSS06jPhoGIOmkNmh3ISiWhhGiJyCytIVOVRT6gcHxYQbQcFBTaLygKnQjIZDEFIYBQIiEJ4f9Tn9yo0uOSkUINRlKVMgXRe+G0bsoN0LQycZRMEsONiv6qJLFN8tbTs3t9JBTtGJgEgSmVsbXNSIbmMyDZ02xTm4N6VLyjqVRM09IWTCRRrDD72W3ay65KTyWn/ZjiROqXLaQqZ49pxqkmgx+XGBNhI22SNxSm3j61Go0OpJNNwDKNZBKZTeyoCz3G6jvCvfLmeBfunB4pb7vXb6W2paRKJNR2m9jd3qqq0BpbrNlAEIQAOYiFUfGqcmramPkvzqKR0FtoYAvJo6NG3gPHFilblwOylEPhEC2EZUT7EzBBJzieJUrlb5SyZ8FI3qV9Q7/sI0XOxfa49oIe1263ywV84F52Hri7tHiFp3hiOVYtxKhQYbC6RicfrCZiOlAmBVXTXepSNx/feCLvX0Lvo2Dm4N+E8cXshLxfC2D2lZESdsqMSawAgmyQDYMAL7SAeVdGPex0z9S/OG7aqo0fzDq8Z3H3GxM/X9UplteDSYNOLldJkafHVfT029vcrXLtgaX3j9uinZy+q/987YUfTm16UDiznf7lLLWV/iheM6Gy9Qh19NUjJxeXFc390lj3/dymF9/9e3vlF+/3v3Rg7eF1h+Cc2Cvjzj25Z7f1zJ+vHf9074lpi8DFg42rV66++MjJy5QY2bLj4a1VO0/Hf+Xwzmnv7Nt/4NvLH25pfT7Yu3N1330ryxcpD13S/jDeurOP7jj6rFS551RZZ2X1js3H5DgQX/754/f2v3B+2Tc14defaL5QNuOr1m3amb5N3wnBlt42qb+80l3avL6JOn1Ob3uuZlbd5Kembr5j/ZSZvxkH37zUeq7P9/VPHbWfzJkCg8fm72r6cfZdzWtneA59pk1dx+/rCmfL9w+Jt2dh5REAAA=='

GLOBALLOCK = multiprocessing.Lock()

headers = {'Authorization': token}

baseUrl = "https://api.ebay.com/buy/browse/v1/item_summary/search?category_ids=63861&conditions:NEW&buyingOptions:FIXED_PRICE&limit=200&aspect_filter=categoryId:63861"

def callBrowseUrl(url,headers):
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            # This means something went wrong.
            raise Error('GET /tasks/ {}'.format(resp.status_code))
        return resp.json()
    except Exception as exc:
        print('%r generated an exception: %s' % (url, exc))


def callBrowse(offset,brand, url,headers,csvwriter):
    finalUrl = "{}&offset={}".format(baseUrl,offset)
    json = callBrowseUrl(finalUrl,headers)
    try:
        GLOBALLOCK.acquire()
        for itemSummary in json['itemSummaries']:
            csvwriter.writerow([offset,brand,itemSummary['itemId'], itemSummary['itemHref']])
        GLOBALLOCK.release()
        return json['total']
    except Exception as exc:
        print('%r generated an exception on csv: %s' % (url, exc))
        GLOBALLOCK.release()

def readBrands():
    with open(BRAND_REFINEMENTS_FILE,encoding="utf-8") as json_file:
        data = json.load(json_file)
        return data['aspectValueDistributions']

def downloadBrand(brand, baseUrl, headers, csvwriter):
    print("downloading brand {}".format(brand))
    brandUrl = "{}&aspect_filter=categoryId:63861,Brand:{}".format(baseUrl,brand)
    offset = 0
    total = callBrowse(offset,brand, brandUrl,headers,csvwriter)
    remaining = total - PAGE_SIZE
    while remaining > 0:
        offset = offset + PAGE_SIZE
        callBrowse(offset,brandUrl,headers,csvwriter)
        remaining = remaining - PAGE_SIZE

#MAIN starts here
brands = readBrands()

with open('result.tsv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter='\t')
    with ThreadPoolExecutor(max_workers = THREADS) as executor:
        for brandRefinement in brands:
            brand = brandRefinement['localizedAspectValue']
            executor.submit(downloadBrand,brand,baseUrl,headers,csvwriter)
