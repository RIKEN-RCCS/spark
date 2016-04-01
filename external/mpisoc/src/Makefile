# Makefile

CP = .:../yampi/trunk/mpijava/java/mpi.jar

all::
	javac -cp $(CP) -d . MPIMux.java MPISoc.java

run::
	mpirun -np 2 java -ea -cp $(CP) TestSocket mt65 3000

echo::
	javac -cp $(CP) TestSocket.java

clean::
	rm -f *.class
	rm -rf mpi

FILES = Makefile MPIMux.java MPISoc.java TestSocket.java

backup::
	(date=`date +'%y%m%d'`; jar cMf 0bak.$$date.jar $(FILES))
