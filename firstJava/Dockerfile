FROM openjdk:7
COPY . /usr/src/myapp
WORKDIR /usr/src/myapp
RUN javac -cp jsch-0.1.54.jar:. Shell.java
CMD ["java", "-cp", "jsch-0.1.54.jar:.", "Shell"]
