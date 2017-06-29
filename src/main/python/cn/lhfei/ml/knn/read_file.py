'''
Created on Sep 12, 2016

@author: lihefei
'''
import csv

f = open('C:/Users/lihefei/Downloads/bank.csv', 'rb')
print(f.readline())




def readCsv():
    
    import csv
with open('eggs.csv', 'wb') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=' ',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
    spamwriter.writerow(['Spam'] * 5 + ['Baked Beans'])
    spamwriter.writerow(['Spam', 'Lovely Spam', 'Wonderful Spam'])