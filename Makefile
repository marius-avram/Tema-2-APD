build: 
	javac *.java
run:
	java Main 4 input.txt output.txt
clean:
	$(RM) *.class
