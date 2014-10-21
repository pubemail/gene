import re
import sys
import os
import MySQLdb


root = 'match_result'

# Build the database
def BuildDB(file, tableName):
	try:
		conn=MySQLdb.connect(host='localhost',user='root',passwd='pmdc12345',db='FCC',port=3306)
	except Exception,e:
		print Exception,e
		sys.exit(1)

	cur = conn.cursor()

	f = open(file,'r')
	idSuffix = file.split('/')[-1]
	
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
		query = 'insert into '+tableName+' values("'+str(linenum)+'@'+idSuffix+'","'+position+'","'+pattern+'","'+data[0]+'","'+data[1]+'","'+data[2]+'","'+data[3]+'")'
		
		linenum = linenum + 4
		
		try:
			cur.execute(query)
		except Exception,e:
			print Exception,e
			sys.exit(1)
	
	cur.close()
	conn.close()


# This function will also establish the file structure
def GetPatterns(patternFile):
	if os.path.exists(root) == False:
		os.mkdir(root)

	patterns = {}
	with open(patternFile, 'r') as f:
		for line in f:
			patterns[line.split()[-1].upper()] = line.split()[0]
		f.close()

	for p in patterns:
		directory = patterns[p]
		if os.path.exists(os.path.join(root, directory)) == False:
			os.mkdir(os.path.join(root, directory))
		filename = patterns[p]+'_0'+'.fq'
		#if os.path.exists(os.path.join(root, directory, filename)) == False:
		f = open(os.path.join(root, directory, filename), 'w')
		filename = patterns[p]+'_1'+'.fq'
		f = open(os.path.join(root, directory, filename), 'w')
		f.close()
	return patterns


def ReadNLine(f, n):
	lines = []
	for i in range(n):
		line = f.readline()
		if len(line) > 0:
			lines.append(line)
	return lines

def Match(seq, patterns, allowError):
	seq = seq.upper()
	for p in patterns:
		p = p.upper()
		regex = '(^'+p+')'
		res = re.findall(regex, seq)
		if len(res) > 0:
			return p
		if allowError == 1:
			for i in range(len(p)):
				regex = '(^'+p[:i]+'.'+p[i+1:]+')'
				res = re.findall(regex, seq)
				if len(res) > 0:
					return p + '.' # '.' means match with one error

	return ''	



def MatchSQL(fileA, fileB, tableB):
	try:
		conn=MySQLdb.connect(host='localhost',user='root',passwd='pmdc12345',db='FCC',port=3306)
	except Exception,e:
		print Exception,e
		sys.exit(1)

	cur = conn.cursor()

	fA = open(fileA,'r')
#	fB = open(fileB,'w')

	
	while True:
		lines = ReadNLine(fA,4)
		if not len(lines) > 0:
			break
		position = re.findall(':(\d{3,}:\d{3,}:\d{3,})', lines[0])[0]
		query = "select * from "+tableB+" where position=\""+position+"\";"
		try:
			cur.execute(query)
		except Exception,e:
			print Exception,e
			sys.exit(1)
	
	cur.close()
	conn.close()



def PrintUsage():
	print '[usage] python fq_splitter.py (input file) (pattern file)'
	print '[clean result] python fq_splitter.py clean'

	

if __name__=='__main__':
	BuildDB('../fqdata/FCC_L8_2.fq', 'FCC_L8_2')
#	MatchSQL('tFCC_L8_1.fq', 'tFCC_L8_2.fq', 'FCC_L8_1')

	sys.exit(0)

	if len(sys.argv) == 2 and sys.argv[1]=='clean':
		if os.system('rm -rf '+root) == 0:
			print 'Clean successfully'
		else:
			print 'Error in clean'
		sys.exit(0)
	elif len(sys.argv) < 3:
		PrintUsage()
		sys.exit(1)

	inputFile = sys.argv[1] #'test.fq'
	patternFile = sys.argv[2] #'barcode.txt'
	allowError = 1

	patterns = GetPatterns(patternFile)
	#print patterns
	
	f = open(inputFile, 'r')
	print 'Matching... please wait'
	while True:
		lines = ReadNLine(f,4)
		if not len(lines) > 0:
			break
		match = Match(lines[1], patterns, allowError)
		if len(match) > 0:
			directory = patterns[match.strip('.')]
			if match.find('.') > 0:
				filename = directory + '_1' + '.fq'
			else:
				filename = directory + '_0' + '.fq'
			fres = open(os.path.join(root,directory,filename), 'a')
			for i in range(len(lines)-1):
				fres.write('%s\n' %lines[i].strip('\n'))
			fres.write('%s\n' %lines[-1].strip('\n'))
	f.close()
		
