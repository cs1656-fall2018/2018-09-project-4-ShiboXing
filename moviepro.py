import sqlite3 as lite
import csv
import re
con = lite.connect('cs1656.sqlite')

with con:
	cur = con.cursor() 

	########################################################################		
	### CREATE TABLES ######################################################
	########################################################################		
	# DO NOT MODIFY - START 
	cur.execute('DROP TABLE IF EXISTS Actors')
	cur.execute("CREATE TABLE Actors(aid INT, fname TEXT, lname TEXT, gender CHAR(6), PRIMARY KEY(aid))")

	cur.execute('DROP TABLE IF EXISTS Movies')
	cur.execute("CREATE TABLE Movies(mid INT, title TEXT, year INT, rank REAL, PRIMARY KEY(mid))")

	cur.execute('DROP TABLE IF EXISTS Directors')
	cur.execute("CREATE TABLE Directors(did INT, fname TEXT, lname TEXT, PRIMARY KEY(did))")

	cur.execute('DROP TABLE IF EXISTS Cast')
	cur.execute("CREATE TABLE Cast(aid INT, mid INT, role TEXT)")

	cur.execute('DROP TABLE IF EXISTS Movie_Director')
	cur.execute("CREATE TABLE Movie_Director(did INT, mid INT)")
	# DO NOT MODIFY - END

	########################################################################		
	### READ DATA FROM FILES ###############################################
	########################################################################		
	# actors.csv, cast.csv, directors.csv, movie_dir.csv, movies.csv
	# UPDATE THIS
	def insertInto(filename,tablename):
		with open(filename,'r') as f:
			reader=csv.reader(f)
			for x in reader:
				query='INSERT INTO '+tablename+' values('
				for y in x[:-1]:
					#try:
					#	float(y)
					#	query+=y+','
					#except ValueError:
					query+="'"+y+"',"
						
				query+="'"+x[-1]+"')"
				#print("query: ",query)
				cur.execute(query)

	########################################################################		
	### INSERT DATA INTO DATABASE ##########################################
	########################################################################		
	# UPDATE THIS TO WORK WITH DATA READ IN FROM CSV FILES
	
	insertInto('actors.csv','Actors')
	insertInto('movies.csv','Movies')
	insertInto('cast.csv','Cast')
	insertInto('directors.csv','Directors')
	insertInto('movie_dir.csv','Movie_Director')
	
	con.commit()
	########################################################################		
	### QUERY SECTION ######################################################
	########################################################################		
	queries = {}

	# DO NOT MODIFY - START 	
	# DEBUG: all_movies ########################
	queries['all_movies'] = '''
SELECT * FROM Movies
'''	
	# DEBUG: all_actors ########################
	queries['all_actors'] = '''
SELECT * FROM Actors
'''	
	# DEBUG: all_cast ########################
	queries['all_cast'] = '''
SELECT * FROM Cast
'''	
	# DEBUG: all_directors ########################
	queries['all_directors'] = '''
SELECT * FROM Directors
'''	
	# DEBUG: all_movie_dir ########################
	queries['all_movie_dir'] = '''
SELECT * FROM Movie_Director
'''	
	# DO NOT MODIFY - END

	########################################################################		
	### INSERT YOUR QUERIES HERE ###########################################
	########################################################################		
	# NOTE: You are allowed to also include other queries here (e.g., 
	# for creating views), that will be executed in alphabetical order.
	# We will grade your program based on the output files q01.csv, 
	# q02.csv, ..., q12.csv

	# Q01 ########################		
	queries['q01'] = '''

	SELECT DISTINCT fname, lname FROM Actors NATURAL JOIN Cast as C
	WHERE aid IN (SELECT aid FROM Cast NATURAL JOIN Movies WHERE year>=1990 and year<=1999) AND
		  aid IN (SELECT aid FROM Cast NATURAL JOIN Movies WHERE year>2009)
	ORDER BY lname ASC, fname ASC

'''	

	
	# Q02 ########################		
	queries['q02'] = '''
	
	SELECT title,year FROM Movies 
	WHERE rank>(SELECT rank FROM Movies WHERE title="Star Wars VII: The Force Awakens" LIMIT 1) AND
	year=(SELECT year FROM Movies WHERE title="Star Wars VII: The Force Awakens" LIMIT 1)
	ORDER BY title ASC
	
'''	

	# Q03 ########################		
	queries['q03'] = '''
	SELECT fname,lname FROM Actors NATURAL JOIN Cast
	WHERE mid in (SELECT mid FROM Movies NATURAL JOIN Cast WHERE title LIKE '%Star Wars%')
	GROUP BY fname,lname
	ORDER BY COUNT(DISTINCT mid) DESC
'''	

	# Q04 ########################		
	queries['q04'] = '''
	SELECT DISTINCT fname,lname FROM ACTORS NATURAL JOIN Cast
	WHERE NOT aid IN (SELECT aid FROM Movies NATURAL JOIN Cast WHERE year>=1987)
	ORDER BY lname ASC, fname ASC
'''	

	# Q05 ########################		
	queries['q05'] = '''
	SELECT DISTINCT fname,lname,COUNT(DISTINCT mid) as num FROM Directors NATURAL JOIN Movie_Director
	GROUP BY did
	ORDER BY num DESC
	LIMIT 20
'''	

	# Q06 ########################	
	

	queries['q06'] = '''
	
	SELECT mid,title,num FROM 
		(SELECT mid,COUNT(DISTINCT aid) AS num FROM Cast  
		GROUP BY mid)
	NATURAL JOIN Movies
	WHERE num IN 
		(SELECT num FROM 
			(SELECT mid,COUNT(DISTINCT aid) AS num FROM Cast
			GROUP BY mid
			LIMIT 20)
		)
	ORDER BY num DESC

'''	

	# Q07 ########################		
	queries['q07'] = '''
		
	SELECT M.mid,title,IFNULL(Female.cnt,0) AS f, IFNULL(Male.cnt,0) AS m
	FROM Movies as M
	LEFT JOIN
		(SELECT mid, COUNT(DISTINCT aid) as cnt
		FROM Cast NATURAL JOIN Actors
		WHERE gender='Female'
		GROUP BY mid) as Female ON M.mid=Female.mid
	LEFT JOIN
		(SELECT mid, COUNT(DISTINCT aid) as cnt
		FROM Cast NATURAL JOIN Actors
		WHERE gender='Male'
		GROUP BY mid) as Male ON M.mid=Male.mid 
	WHERE f>m
'''

	# Q08 ########################		
	queries['q08'] = '''
	SELECT fname,lname,cnt FROM
		(SELECT A.aid,COUNT(DISTINCT MD.did) as cnt
		FROM Cast as C NATURAL JOIN Movie_Director as MD 
		LEFT JOIN Directors as D ON MD.did=D.did
		LEFT JOIN Actors as A ON C.aid=A.aid
		WHERE NOT (D.fname=A.fname AND D.lname=A.lname)
		GROUP BY A.aid)
	NATURAL JOIN Actors
	WHERE cnt>=6
	ORDER BY cnt DESC

'''	

	# Q09 ########################		
	queries['q09'] = '''
	SELECT fname,lname,cnt FROM
		(SELECT aid,COUNT(mid) as cnt FROM
			(SELECT aid, MIN(year) as min_y
			FROM Cast NATURAL JOIN Movies
			GROUP BY aid)
		NATURAL JOIN Actors
		NATURAL JOIN Cast
		NATURAL JOIN Movies
		WHERE year = min_y
		GROUP BY aid)
	NATURAL JOIN Actors
	WHERE fname LIKE 'S%'
	ORDER BY cnt DESC
	
'''	

	# Q10 ########################		
	queries['q10'] = '''
	SELECT lname, title
	FROM Movie_Director MD NATURAL JOIN Directors NATURAL JOIN Movies
	WHERE lname IN 
		(SELECT lname FROM Cast C NATURAL JOIN Actors A 
		WHERE C.mid=MD.mid)
	ORDER BY lname
	
'''	
	queries['q10a']='''
		SELECT mid FROM Cast NATURAL JOIN Actors
		WHERE fname='Tom' AND lname='Hanks'
'''

	queries['q10b']='''
	CREATE VIEW BACON_2 AS
	SELECT DISTINCT C.aid,C.mid FROM
		(SELECT aid,mid FROM Cast NATURAL JOIN Actors
		WHERE mid IN
			(SELECT mid FROM Cast NATURAL JOIN Actors
			WHERE fname='Tom' AND lname='Hanks')
		AND NOT (fname='Tom' AND lname='Hanks')) First
	,Cast C
	WHERE C.aid=First.aid 
	AND NOT C.mid 
			IN (SELECT mid FROM Cast NATURAL JOIN Actors
			WHERE fname='Tom' AND lname='Hanks')
'''

	# Q11 ########################		
	queries['q11'] = '''
	SELECT DISTINCT fname,lname
	FROM BACON_2 B,Cast C,Actors A
	WHERE C.mid=B.mid AND NOT C.aid IN (SELECT aid FROM BACON_2)
		  AND C.aid=A.aid 
	ORDER BY lname ASC, fname ASC

'''	

	queries['q11a']='''
		DROP VIEW IF EXISTS BACON_2
'''

	# Q12 ########################		
	queries['q12'] = '''
	SELECT fname,lname,cnt FROM Actors  NATURAL JOIN
		(SELECT aid,AVG(rank) cnt FROM Cast NATURAL JOIN Movies
		GROUP BY aid)
	ORDER BY cnt DESC
	LIMIT 20
	
'''	


	########################################################################		
	### SAVE RESULTS TO FILES ##############################################
	########################################################################		
	# DO NOT MODIFY - START 	
	for (qkey, qstring) in sorted(queries.items()):
		try:
			cur.execute(qstring)
			all_rows = cur.fetchall()
			
			print ("=========== ",qkey," QUERY ======================")
			print (qstring)
			print ("----------- ",qkey," RESULTS --------------------")
			for row in all_rows:
				print (row)
			print (" ")

			save_to_file = (re.search(r'q0\d', qkey) or re.search(r'q1[012]', qkey))
			if (save_to_file):
				with open(qkey+'.csv', 'w') as f:
					writer = csv.writer(f)
					writer.writerows(all_rows)
					f.close()
				print ("----------- ",qkey+".csv"," *SAVED* ----------------\n")
		
		except lite.Error as e:
			print ("An error occurred:", e.args[0])
	# DO NOT MODIFY - END
	
