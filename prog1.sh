 "Starting script ..."

[ -s /mr-quads/quad1 ]
if [[ $? == 0 ]]; then
	ls -lrt /mr-quads/quad1
	echo "Cleaning up directory /mr-quads/quad1"
	rm -r -f /mr-quads/quad1
	echo "Return code $?"
	
fi

[ -s /mr-quads/quad2 ]
if [[ $? == 0 ]]; then
        ls -lrt /mr-quads/quad2
        echo "Cleaning up directory /mr-quads/quad2"
        rm -r -f /mr-quads/quad2
        echo "Return code $?"

fi


echo "Compiling program prog1.java..."
javac -cp $(hadoop classpath) /mr-quads/prog1.java -d /mr-quads/prog1Classes
if [[ $? == 1 ]]; then
  echo " Compile failed!"
  exit  1
fi


echo "Creating jar file from the class binaries..."
jar -cvf /mr-quads/prog1.jar -C /mr-quads/prog1Classes/ ./
if [[ $? == 1 ]]; then
  echo " jar creation failed!"
  exit 1
fi

echo "Running hadoop program"
hadoop jar prog1.jar prog1 
if [[ $? == 1 ]]; then
  echo  "running hadoop mr job failed!"
  exit 1
fi

echo "All-good, Exiting master script"

ls -lrt /mr-quads/quad1

ls -lrt /mr-quads/quad2

