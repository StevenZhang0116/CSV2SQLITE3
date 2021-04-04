#!/usr/bin/python
#coding=UTF-8

from .db_handler import *
import time
import traceback

__all__=['csv_reader','csv_db']

class csv_reader:
	'''
	'''
	def __init__(self):
		'''
		'''
		self.fp=None
		return

	def log(self,msg):
		'''
		'''
		sval=f'[csv_reader]{msg}'
		print(f'{sval}')
		return

	def open(self,filepath):
		'''
		'''

		self.log(f'[open]:filepath({filepath})')

		ret=True
		try:
			self.fp=open(filepath,'r')
			ret=True
		except Exception as ex:
			self.log(f'[open][exception]:({ex})')
			ret=False
			self.fp=None

		self.log(f'[open]:done.ret({ret})')

		return ret

	def readline(self):
		'''
		'''
		ret=None

		try:
			if self.fp is not None:
				ret=self.fp.readline()
				if ret is not None:
					if len(ret)<=0:
						ret=None
					else:
						ret=self.build_fields(ret)

		except Exception as ex:
			traceback.print_exc()
			self.log(f'[readline][exception]:({ex})')
			ret=None

		return ret

	def build_fields(self,line):
		'''
		'''
		ret=None
		line=line.strip()
		a=line.split(',')
		ret = []
		# print(f"line: {line}")
		
		index = 0
		while index < len(a):
			val = a[index]
			if len(val) <= 0:
				ret.append(val)
				index += 1
				continue 

			if val[0] == '"':
				if val[-1] == '"':
					ret.append(val[1:-1])
					index += 1
					continue 
				else:
					sval = val 
					index += 1
					while index < len(a):
						cval = a[index]
						index += 1
						sval += cval
						if cval[-1] == '"':
							break
					ret.append(sval[1:-1])
					continue
			ret.append(val)
			index += 1
			# print(ret)
		# print(f"ret:{ret}")
		# print("##########")

		return ret

	def close(self):
		'''
		'''
		self.log(f'[close].')

		try:
			if self.fp is None:
				pass
			else:
				close(self.fp)
		except Exception as ex:
			pass

		self.log(f'[close]:done.')

		return True

class csv_db:
	'''
	'''
	def __init__(self):
		'''
		'''
		self.rec_cnt=0
		self.commit_cnt=0
		self.prepared=False

		self.first_row_as_field=True

		self.fpath=None
		self.dh=None

		return

	def log(self,msg):
		'''
		'''
		sval=f'[csv_db]{msg}'
		print(f'{sval}')
		return

	def open(self,fpath):
		'''
		'''
		self.log(f'[open]:fpath({fpath})')

		self.rec_cnt=0
		self.commit_cnt=0
		self.fpath=fpath
		self.prepared=False
		return True

	def get_handle(self):
		'''
		'''
		ret=None
		if self.fpath is not None:
			ret=DBHandler({'filepath':self.fpath})
		else:
			self.log('[get_handle][error]:no filepath.')

		return ret

	def write(self,tname,data):
		'''
		'''
		ret=True
		add=True

		# self.log(f'[write]:table({tname}) data({data})')

		if not self.prepared:
			if self.first_row_as_field:
				ret=self.create_table(tname,data)
				add=False
			else:
				cols=[f'F{i:02d}' for i in range(len(data))]
				ret=self.create_table(tname,cols)
				add=True
			if ret:
				self.prepared=True

		if ret and add:
			ret=self.insert_data(tname,data)
			if ret:
				ret=self.check_commit()

		return ret

	def check_commit(self):
		'''
		'''
		ret=True
		self.commit_cnt+=1
		if self.commit_cnt<100:
			return ret

		try:
			self.dh.commit()
			self.dh.begin()
			self.commit_cnt=0
			ret=True			
		except Exception as ex:
			ret=False
			self.dh.close()
			self.dh=None
			self.log(f'[check_commit][exception]:({ex})')

		self.log(f'[check_commit]:done.ret({ret})')

		return ret

	def insert_data(self,tname,data):
		'''
		'''
		ret=True
		dh=None

		vval=['?' for _ in range(len(data))]
		sval=','.join(vval)

		sql=f'''
		insert into {tname} values({sval})
		'''

		try:
			dh=self.dh
			
			if dh is None:
				dh=self.get_handle()
				dh.open()
				dh.begin()
				self.dh=dh

			dh.execute(sql,data)
			ret=True
			self.rec_cnt+=1

		except Exception as ex:
			ret=False
			dh.rollback()
			dh.close()
			self.dh=None
			self.log(f'[insert_data][exception]:({ex})')

		self.log(f'[insert_data]:done.ret({ret}) cnt({self.rec_cnt})')
		# print(data)
		# print(len(data))
		traceback.print_exc()

		return ret


	def create_table(self,tname,cols):
		'''
		'''
		flds=[]

		for col in cols:
			fval=f'{col} varchar(64)'
			flds.append(fval)

		sflds=','.join(flds)
		sql=f'''
			create table if not exists {tname} ({sflds})
		'''

		ret=True

		dh=self.get_handle()
		try:
			dh.open()
			dh.execute(sql)
			dh.close()
			ret=True
		except Exception as ex:
			ret=False
			self.log(f'[create_table][exception]:({ex})')
			dh.close()

		self.log(f'[create_table]:done.ret({ret})')

		return ret

	def close(self):
		'''
		'''

		self.log(f'[close].')

		if self.dh is None:
			self.log(f'[close]:done.')
			return True

		try:
			self.dh.commit()
			self.dh.close()
			self.dh=None
		except Exception as ex:
			self.dh.close()
			self.dh=None
			self.log(f'[close][exception]:({ex})')

		self.log(f'[close]:done.')

		return True







