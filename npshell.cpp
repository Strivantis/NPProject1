#include <iostream>
#include <vector>
#include <string>
#include <cstdlib>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <cstring>
#include <sstream>
#include <algorithm>
#include <unistd.h>

using namespace std;

struct MemoryPipeData {
    int creationID;
    int countDown;
    bool pipeStderr;
    string data;
};

static int globalCreationID = 0;
static vector<MemoryPipeData> memoryPipeTable;

struct CommandUnit {
    vector<string> args;
    bool pipeToNext = false;
    bool pipeToNextStderr = false;
    int pipeNumber = 0;
    bool redirect = false;
    string redirectFile;
    bool newGroup = false;
};

void flushFdToStdout(int fd) {
    if (fd < 0) return;
    string flushed;
    char buffer[4096];
    ssize_t n;
    while ((n = read(fd, buffer, sizeof(buffer))) > 0) {
        flushed.append(buffer, n);
    }
    close(fd);
    if (!flushed.empty() && flushed.back() != '\n') {
        flushed.push_back('\n');
    }
    cout << flushed;
    cout.flush();
}

vector<string> tokenize(const string &input) {
    vector<string> tokens;
    istringstream iss(input);
    string token;
    while (iss >> token)
        tokens.push_back(token);
    return tokens;
}

vector<CommandUnit> parseLine(const string &input) {
    vector<CommandUnit> result;
    vector<string> tokens = tokenize(input);

    bool inNewGroup = true;
    CommandUnit currentCmd;
    bool invalidPipeFound = false;

    auto flushCurrentCmd = [&](bool pushNewGroup) {
        if (!currentCmd.args.empty() || currentCmd.redirect) {
            if (inNewGroup) {
                currentCmd.newGroup = true;
                inNewGroup = false;
            }
            currentCmd.pipeNumber = 0;
            currentCmd.pipeToNextStderr = false;
            result.push_back(currentCmd);
        }
        currentCmd = CommandUnit();
        if (pushNewGroup) inNewGroup = true;
    };

    auto setNumberedPipeForPrevCmd = [&](int n, bool toStderr) {
        if (!result.empty()) {
            result.back().pipeNumber = n;
            result.back().pipeToNextStderr = toStderr;
            result.back().pipeToNext = true; 
        }
    };

    auto setPipeForPrevCmd = [&]() {
        if (!result.empty()) {
            result.back().pipeToNext = true;
        }
    };

    size_t i = 0;
    while (i < tokens.size()) {
        const string &tk = tokens[i];
        if ((tk.size() >= 2) && (tk[0] == '|' || tk[0] == '!')) {
            int num = stoi(tk.substr(1));
            if (num < 1 || num > 1000) {
                cerr << "Invalid pipe number: " << num << ". Must be between 1 and 1000.\n";
                invalidPipeFound = true;
                break;
            }
            flushCurrentCmd(false);
            bool stderrFlag = (tk[0] == '!');
            setNumberedPipeForPrevCmd(num, stderrFlag);
            inNewGroup = true;
            i++;
            continue;
        }
        else if (tk == "|") {
            flushCurrentCmd(false);
            setPipeForPrevCmd();
            i++;
            continue;
        }
        else if (tk == ">") {
            i++;
            if (i < tokens.size()) {
                currentCmd.redirect = true;
                currentCmd.redirectFile = tokens[i];
                i++;
            } else {
                cerr << "Error: Missing file after '>'\n";
            }
        }
        else {
            currentCmd.args.push_back(tk);
            i++;
        }
    }

    if (invalidPipeFound) {
        return vector<CommandUnit>();
    }

    flushCurrentCmd(false);
    return result;
}

class MemoryPipeManager {
public:
    static vector<MemoryPipeData> memoryPipeTable;

    static void decreaseCountdown() {
        for (auto &mp : memoryPipeTable) {
            mp.countDown--;
        }
    }

    static int getInputFD(bool consume = true) {
        vector<size_t> zeroIndex;
        for (size_t i = 0; i < memoryPipeTable.size(); i++) {
            if (memoryPipeTable[i].countDown == 0)
                zeroIndex.push_back(i);
        }
        if (zeroIndex.empty())
            return -1;

        int aggregator[2];
        pipe(aggregator);
        string combined;
        for (size_t i = 0; i < zeroIndex.size(); i++) {
            combined.append(memoryPipeTable[zeroIndex[i]].data);
        }
        if (!combined.empty() && combined.back() != '\n') {
            combined.push_back('\n');
        }
        if (consume) {
            for (int i = zeroIndex.size() - 1; i >= 0; i--) {
                memoryPipeTable.erase(memoryPipeTable.begin() + zeroIndex[i]);
            }
        }

        pid_t pid = fork();
        if (pid < 0) {
            close(aggregator[0]); close(aggregator[1]);
            return -1;
        } else if (pid == 0) {
            pid_t pid2 = fork();
            if (pid2 < 0) exit(EXIT_FAILURE);
            if (pid2 > 0) exit(0);
            close(aggregator[0]);
            size_t totalWritten = 0;
            while (totalWritten < combined.size()) {
                ssize_t written = write(aggregator[1], combined.data() + totalWritten, combined.size() - totalWritten);
                if (written < 0) break;
                totalWritten += written;
            }
            close(aggregator[1]);
            exit(0);
        } else {
            int status;
            waitpid(pid, &status, 0);
            close(aggregator[1]);
            return aggregator[0];
        }
    }
};

bool handleBuiltInCommands(const vector<string> &tokens, bool &shouldExit) {
    if (tokens.empty()) return true;
    const string &cmd = tokens[0];
    if (cmd.size() >= 4 && cmd.rfind("exit", 0) == 0) {
        int fd = MemoryPipeManager::getInputFD(true);
        flushFdToStdout(fd);
        exit(0);
    }
    if (cmd == "setenv" && tokens.size() == 3) {
        setenv(tokens[1].c_str(), tokens[2].c_str(), 1);
        return true;
    }
    if (cmd == "printenv" && tokens.size() == 2) {
        char* v = getenv(tokens[1].c_str());
        if (v) cout << v << endl;
        return true;
    }
    return false;
}

vector<MemoryPipeData> MemoryPipeManager::memoryPipeTable;

int runSingleCommand(const CommandUnit &cmd, int inFd, vector<pid_t> &childPids) {
    int outFd = -1, pipeFd[2] = {-1, -1};
    bool localPipeStderr = false;
    if (cmd.pipeToNext || cmd.pipeNumber > 0) {
        pipe(pipeFd);
        outFd = pipeFd[1];
        localPipeStderr = cmd.pipeToNextStderr;
    }

    int redirectPipeReadEnd = -1;
    if (cmd.redirect) {
        int redir[2];
        pipe(redir);
        redirectPipeReadEnd = redir[0];
        outFd = redir[1];
    }

    int dummyPipe[2] = {-1, -1};
    if (inFd == -1) {
        pipe(dummyPipe);
        close(dummyPipe[1]);
        inFd = dummyPipe[0];
    }

    vector<char*> argv;
    for (auto &s : cmd.args) {
        argv.push_back(const_cast<char*>(s.c_str()));
    }
    argv.push_back(nullptr);

    pid_t pid = fork();
    if (pid < 0) {
        cerr << "Fork failed.\n";
        exit(EXIT_FAILURE);
    } else if (pid == 0) {
        if (inFd != -1) dup2(inFd, STDIN_FILENO);
        if (outFd != -1) {
            dup2(outFd, STDOUT_FILENO);
            if (localPipeStderr) dup2(outFd, STDERR_FILENO);
        }
        execvp(argv[0], argv.data());
        cerr << "Unknown command: [" << argv[0] << "]." << endl;
        exit(EXIT_FAILURE);
    }

    childPids.push_back(pid);
    if (inFd != -1) close(inFd);
    if (outFd != -1) close(outFd);

    int inputForNextCommand = -1;
    if (cmd.pipeNumber > 0) {
        string resultData;
        char buffer[4096];
        ssize_t n;
        while ((n = read(pipeFd[0], buffer, sizeof(buffer))) > 0)
            resultData.append(buffer, n);
        close(pipeFd[0]);
        MemoryPipeManager::memoryPipeTable.push_back({
            globalCreationID++, cmd.pipeNumber, cmd.pipeToNextStderr, move(resultData)
        });
        inputForNextCommand = -1;
    } else {
        inputForNextCommand = cmd.pipeToNext ? pipeFd[0] : -1;
    }

    if (cmd.redirect) {
        string filedata;
        char buffer[4096];
        ssize_t n;
        while ((n = read(redirectPipeReadEnd, buffer, sizeof(buffer))) > 0)
            filedata.append(buffer, n);
        close(redirectPipeReadEnd);
        int fdout = open(cmd.redirectFile.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fdout >= 0) {
            write(fdout, filedata.data(), filedata.size());
            close(fdout);
        } else {
            cerr << "Failed to open file: " << cmd.redirectFile << endl;
        }
    }

    return inputForNextCommand;
}

int mergeInputFDs(int fd1, int fd2) {
    if (fd1 < 0 && fd2 < 0) return -1;
    int aggregator[2];
    pipe(aggregator);
    auto copyAll = [&](int srcFd) {
        if (srcFd < 0) return;
        char buf[4096];
        ssize_t n;
        while ((n = read(srcFd, buf, sizeof(buf))) > 0)
            write(aggregator[1], buf, n);
        close(srcFd);
    };
    copyAll(fd1);
    copyAll(fd2);
    close(aggregator[1]);
    return aggregator[0];
}

vector<vector<CommandUnit>> splitGroups(const vector<CommandUnit>& commands) {
    vector<vector<CommandUnit>> groups;
    vector<CommandUnit> currentGroup;
    for (const auto &cmd : commands) {
        if (cmd.newGroup && !currentGroup.empty()) {
            groups.push_back(currentGroup);
            currentGroup.clear();
        }
        currentGroup.push_back(cmd);
    }
    if (!currentGroup.empty())
        groups.push_back(currentGroup);
    return groups;
}

void executeLineCommands(vector<CommandUnit> &commands, int firstInFd) {
    int inFd = firstInFd;
    vector<pid_t> childPids;
    const size_t MAX_CHILDREN = 200;
    for (size_t i = 0; i < commands.size(); i++) {
        inFd = runSingleCommand(commands[i], inFd, childPids);
        while (childPids.size() >= MAX_CHILDREN) {
            int status;
            pid_t finished = waitpid(-1, &status, 0);
            auto it = std::find(childPids.begin(), childPids.end(), finished);
            if (it != childPids.end()) childPids.erase(it);
        }
    }
    for (pid_t pid : childPids) {
        int status;
        waitpid(pid, &status, 0);
    }
    if (!commands.empty() && commands.back().pipeNumber > 0) return;
    int finalFD = mergeInputFDs(-1, inFd);
    if (finalFD != -1) {
        string finalOutput;
        char buffer[4096];
        ssize_t n;
        while ((n = read(finalFD, buffer, sizeof(buffer))) > 0) finalOutput.append(buffer, n);
        close(finalFD);
        if (!finalOutput.empty() && finalOutput.back() != '\n') finalOutput.push_back('\n');
        write(STDOUT_FILENO, finalOutput.data(), finalOutput.size());
    }
}

int main() {
    setenv("PATH", "bin:.", 1);
    while (true) {
        cout << "% ";
        cout.flush();
        string line;
        if (!getline(cin, line)) {
            int fd = MemoryPipeManager::getInputFD(true);
            flushFdToStdout(fd);
            break;
        }
        vector<string> tokens = tokenize(line);
        if (tokens.empty()) continue;
        bool shouldExit = false;
        if (!handleBuiltInCommands(tokens, shouldExit)) {
            vector<CommandUnit> commands = parseLine(line);
            vector<vector<CommandUnit>> groups = splitGroups(commands);
            int memoryFd = -1;
            for (auto &group : groups) {
                memoryFd = MemoryPipeManager::getInputFD(true);
                executeLineCommands(group, memoryFd);
                MemoryPipeManager::decreaseCountdown();
            }
        } else {
            MemoryPipeManager::decreaseCountdown();
            int fd = MemoryPipeManager::getInputFD(true);
            flushFdToStdout(fd);
        }
        while (waitpid(-1, nullptr, WNOHANG) > 0) {}
        if (shouldExit) break;
    }
    return 0;
}
