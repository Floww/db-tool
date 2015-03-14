#!/bin/bash
osascript <<END 
tell application "Terminal"
	do script "cd \"`dirname $0`\";java -jar db-tool.jar;"
end tell
END