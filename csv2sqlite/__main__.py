#!/usr/bin/python
#coding=UTF-8

from .csv2sqlite import *

def main_routine():
	'''
	'''
	fpath=f'./CHARTEVENTS.csv'
	outpath=f'./CHARTEVENTS.sqlite3'
	table_name='CHARTEVENTS_TABLE'

	db=csv_db()
	ret=db.open(outpath)

	if not ret:
		return

	reader=csv_reader()
	ret=reader.open(fpath)

	if ret:
		while True:
			line=reader.readline()
			if line is None:
				break

			ret=db.write(table_name,line)
			if not ret:
				break

		reader.close()

	db.close()

	return

if __name__=='__main__':
	main_routine()



