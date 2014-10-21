import MySQLdb
import re


conn=MySQLdb.connect(host='localhost',user='root',passwd='pmdc12345',db='FCC_L8',port=3306)
cur = conn.cursor()

file = 'test.fq'
f = open(file,'r')

linenum = 0
while True:
	data = []
	line =''
	for i in range(4):
		line = f.readline().strip('\n')
		if not len(line)>0:
			break
		if i==3:
			tmp = line
			line = ''
			for i in tmp:
				line = line + str(ord(i)) + ' '
		data.append(line)
	if not len(line)>0:
		break
	
	position = re.findall(':(\d{3,}:\d{3,}:\d{3,})', data[0])[0]
	pattern = data[1][0:11]
	query = 'insert into FCC_L8 values("'+str(linenum)+'@1","'+position+'","'+pattern+'","'+data[0]+'","'+data[1]+'","'+data[2]+'","'+data[3]+'")'
	
	linenum = linenum + 4
	
	cur.execute(query)

cur.close()
conn.close()
