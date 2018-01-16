/*
  Script that simulates a single user on a JupyterHub
  */
const os = require('os');
const request = require('request-promise').defaults({simple: false});
const services = require('@jupyterlab/services');
const ws = require('ws');
const xhr = require('./xhr');
const url = require('url');
const winston = require('winston');
const winstonTcp = require('winston-tcp');
var program = require('commander');
const USERS = [
    ['user0', '2126a6'],
    ['user1', '577f9d'],
    ['user2', '23e7ee'],
    ['user3', '08d13d'],
    ['user4', '694c34'],
    ['user5', '0e0c2e'],
    ['user6', '96d4f3'],
    ['user7', 'd8d890'],
    ['user8', '4ac03f'],
    ['user9', 'bb01e4'],
    ['user10', 'd3f3d0'],
    ['user11', 'afac09'],
    ['user12', '7c44ec'],
    ['user13', '87c070'],
    ['user14', '0d4731'],
    ['user15', '041572'],
    ['user16', '35c1a2'],
    ['user17', '074df8'],
    ['user18', '8e5238'],
    ['user19', '7cf785'],
    ['user20', '9607b6'],
    ['user21', 'd58608'],
    ['user22', '50ad04'],
    ['user23', 'fa7196'],
    ['user24', 'b399b3'],
    ['user25', '09dab4'],
    ['user26', '9a35c7'],
    ['user27', '5f5b2b'],
    ['user28', '4633f4'],
    ['user29', '257784'],
    ['user30', 'c8c8d5'],
    ['user31', '816132'],
    ['user32', 'c273c4'],
    ['user33', 'e70a7b'],
    ['user34', '2ad808'],
    ['user35', '65471d'],
    ['user36', '696fa5'],
    ['user37', '51a631'],
    ['user38', '9d3eaf'],
    ['user39', '4ab245'],
    ['user40', '336c25'],
    ['user41', 'a3c1e0'],
    ['user42', '23acdf'],
    ['user43', '7065e9'],
    ['user44', '7266fb'],
    ['user45', '1ee32b'],
    ['user46', '71b4c1'],
    ['user47', '8da224'],
    ['user48', '73a2ea'],
    ['user49', '1c4259'],
    ['user50', 'e02ba7'],
    ['user51', 'fd3516'],
    ['user52', 'a1a27e'],
    ['user53', '62146f'],
    ['user54', '95edde'],
    ['user55', 'e42076'],
    ['user56', 'ff1c1a'],
    ['user57', 'dbd546'],
    ['user58', '6a08e7'],
    ['user59', '2627a0'],
    ['user60', '6536c9'],
    ['user61', '6845ab'],
    ['user62', '3851c8'],
    ['user63', 'acf89d'],
    ['user64', '4f77a9'],
    ['user65', 'a38420'],
    ['user66', 'a3eff5'],
    ['user67', '350536'],
    ['user68', '258770'],
    ['user69', '615bbe'],
    ['user70', '39d89d'],
    ['user71', '305944'],
    ['user72', '0d6fb2'],
    ['user73', '2558cc'],
    ['user74', 'c8b16e'],
    ['user75', '311601'],
    ['user76', 'ebbcf0'],
    ['user77', 'd77861'],
    ['user78', 'e2fafa'],
    ['user79', '9b2d4d'],
    ['user80', 'fd8d0c'],
    ['user81', '0042d9'],
    ['user82', '350c04'],
    ['user83', '9f9c69'],
    ['user84', 'f952c3'],
    ['user85', 'a0f908'],
    ['user86', 'aaae4b'],
    ['user87', 'd33fc6'],
    ['user88', '84d888'],
    ['user89', 'fabbd4'],
    ['user90', '169696'],
    ['user91', '278abf'],
    ['user92', '7e66b9'],
    ['user93', '71dff6'],
    ['user94', '75849e'],
    ['user95', 'eef033'],
    ['user96', '8c8653'],
    ['user97', '19ba8d'],
    ['user98', '5442b2'],
    ['user99', '7233df']
];

class User {

    constructor(hubUrl, username, password, eventEmitter) {
        this.hubUrl = hubUrl;
        this.username = username;
        this.password = password;
        this.cookieJar = request.jar();
        this.notebookUrl = this.hubUrl + '/user/' + this.username;
        this.eventEmitter = eventEmitter;
    }

    emitEvent(type, duration=0, event={}) {
        event['type'] = type;
        event['timestamp'] = Date.now();
        event['user'] = this.username;
        event['duration'] = duration[0] * 1000 + duration[1] / 1000000;
        this.eventEmitter.info(event);
    }

    async login() {
        let startTime = process.hrtime();
        var postUrl = this.hubUrl + '/hub/login';
        try {
            await request({
                method: 'POST',
                url: postUrl,
                form: {username: this.username, password: this.password},
                jar: this.cookieJar,
                resolveWithFullResponse: true
            });
            let timeTaken = process.hrtime(startTime);
            this.emitEvent('login.success', timeTaken);
        } catch(c) {
            let timeTaken = process.hrtime(startTime);
            this.emitEvent('login.failure' + c, timeTaken);
        }
    }

    async startServer() {
        let startTime = process.hrtime();
        var nextUrl = this.hubUrl + '/hub/spawn';
        for (var i = 0; i < 300; i++) {
            var expectedUrl = this.notebookUrl + '/tree?';
            try {
                var resp = await request({
                    method: 'GET',
                    url: nextUrl,
                    jar: this.cookieJar,
                    followRedirect: function(req) {
                        return true;
                    },
                    resolveWithFullResponse: true
                });
            } catch(e) {
                // LOL @ STATE OF ERROR HANDLING IN JS?!@?
                let timeTaken = process.hrtime(startTime);
                if (e.message.startsWith('Error: Exceeded maxRedirects. Probably stuck in a redirect loop ')) {
                    this.emitEvent('server-start.toomanyredirects', timeTaken);
                } else {
                    console.log(e.stack);
                }
                return false;
            }
            if (resp.request.uri.href == expectedUrl) {
                let timeTaken = process.hrtime(startTime);
                this.emitEvent('server-start.success', timeTaken);
                return true;
            } else {
                nextUrl = resp.request.uri.href;
                await new Promise(r => setTimeout(r, 1000));
            }
        }
        let timeTaken = process.hrtime(startTime);
        this.emitEvent('server-start.failed', timeTaken);
        return false;
    };

    async stopServer() {
        let startTime = process.hrtime();
        let stopUrl = this.hubUrl + '/hub/api/users/' + this.username + '/server';
        let headers = {
            'Referer': this.hubUrl + '/hub'
        };
        try {
            let resp = await request({
                method: 'DELETE',
                url: stopUrl,
                jar: this.cookieJar,
                resolveWithFullResponse: true,
                headers: headers
            });
            let timeTaken = process.hrtime(startTime);
            this.emitEvent('server-stop.success', timeTaken);
        } catch(e) {
            let timeTaken = process.hrtime(startTime);
            this.emitEvent('server-stop.failure', timeTaken);
        }

    }

    startKernel() {
        return new Promise((resolve, reject) => {
            let startTime = process.hrtime();
            let headers = {
                'Cookie': this.cookieJar.getCookieString(this.notebookUrl + '/')
            };
            this.cookieJar.getCookies(this.notebookUrl).forEach((cookie) => {
                if (cookie.key == '_xsrf') { headers['X-XSRFToken'] = cookie.value; };
            });

            let serverSettings = services.ServerConnection.makeSettings({
                xhrFactory: function () { return new xhr.XMLHttpRequest(); },
                wsFactory: function (url, protocol) {
                    return new ws(url, protocol, {'headers': headers});
                },
                requestHeaders: headers,
                baseUrl: this.notebookUrl
            });

            let failure = () => {
                let timeTaken = process.hrtime(startTime);
                this.emitEvent('kernel-start.failure', timeTaken);
                reject();
            };
            services.Kernel.getSpecs(serverSettings).then((kernelSpecs) => {
                this.kernel = services.Kernel.startNew({
                    name: kernelSpecs.default,
                    serverSettings: serverSettings
                }).then((kernel) => {

                    this.kernel = kernel;
                    this.kernel.statusChanged.connect((status) => {
                        if (status.status == 'connected') {
                            resolve();
                        }
                    });
                });
            });
            let timeTaken = process.hrtime(startTime);
            this.emitEvent('kernel-start.success', timeTaken);
        });
    }

    async stopKernel() {
        let startTime = process.hrtime();
        if (this.kernel) {
            await this.kernel.shutdown();
            let timeTaken = process.hrtime(startTime);
            this.emitEvent('kernel-stop.success', timeTaken);
        }
    }

    executeCodeCPU(timeout) {
        // Explicitly *not* an async defined function, since we want to
        // explictly return a Promise.
        let cancelled = false;

        setTimeout(() => { cancelled = true; }, timeout);
        return new Promise((resolve, reject) => {

            let executeFib = () => {
                if (cancelled) {
                    this.emitEvent('code-execute.complete');
                    resolve();
                    return;
                }
                let future = this.kernel.requestExecute({ code: 'fib = lambda n: n if n < 2 else fib(n-1) + fib(n-2); print(fib(20))'} );
                // This will fire if we don't have an answer back from the kernel within 1s
                let startTime = process.hrtime();
                let failureTimer = setTimeout(() => {
                    let timeTaken = process.hrtime(startTime);
                    this.emitEvent('code-execute.timeout', timeTaken);
                    reject();
                }, 1000);
                future.onIOPub = (msg) => {
                    clearTimeout(failureTimer);
                    let timeTaken = process.hrtime(startTime);
                    if (msg.content.text == '6765\n') {
                        setTimeout(executeFib, 1000);
                        this.emitEvent('code-execute.success', timeTaken);
                    }
                };
            };
            executeFib();
        });
    }

    executeCodeRW(timeout) {
        // Explicitly *not* an async defined function, since we want to
        // explictly return a Promise.
        let cancelled = false;

        setTimeout(() => { cancelled = true; }, timeout);
        return new Promise((resolve, reject) => {

            let executeRW = () => {
                if (cancelled) {
                    this.emitEvent('code-execute.complete');
                    resolve();
                    return;
                }
                let future = this.kernel.requestExecute({
                    code: "fp = open('testing_file.txt', 'w'); fp.write('hello_world\n'); fp.close();"
                });
                // This will fire if we don't have an answer back from the kernel within 1s
                let startTime = process.hrtime();
                let failureTimer = setTimeout(() => {
                    let timeTaken = process.hrtime(startTime);
                    this.emitEvent('code-execute.timeout', timeTaken);
                    reject();
                }, 1000);
                future.onIOPub = (msg) => {
                    clearTimeout(failureTimer);
                    let timeTaken = process.hrtime(startTime);
                    if (msg.content.text == '6765\n') {
                        setTimeout(executeFib, 1000);
                        this.emitEvent('code-execute.success', timeTaken);
                    }
                };
            };
            executeRW();
        });
    }

}
function justMetaFormatter(k, v) {
    // Remove the message and level keys, which are automatically added by winston
    if (k == 'message' || k == 'level') { return undefined; };
    return v;
}

function main(hubUrl, userCount) {

    console.log(`Loading ${userCount} users on ${hubUrl}`);
    console.log(`Will complete spawning all users by ${program.usersStartTime}s`);
    console.log(`Users will start dropping off after ${program.minUserActiveTime}s`);
    console.log(`Users will be all gone by ${program.maxUserActiveTime}s`);
    console.log(`Users will have random names prefixed with ${program.userPrefix}`);

    let transports = [
        new winston.transports.Console({
            showLevel: false,
            formatter: (opts) => {
                return JSON.stringify(opts.meta, justMetaFormatter);
            },
        }),
    ];


    if (program.eventsTcpServer) {
        console.log(`Sending event stream data to ${program.eventsTcpServer}`);
        const [host, port] = program.eventsTcpServer.split(':');
        transports.push(
            new winstonTcp({
                host: host,
                port: parseInt(port),
                timestamp: false,
                formatter: (opts) => {
                    return JSON.stringify(opts.meta, justMetaFormatter);
                },
            })
        );
    }

    const eventEmitter = new winston.Logger({
        level: 'info',
        transports: transports
    });

    async function launch(i) {

        // Wait for a random amount of time before actually launching
        await new Promise(r => setTimeout(r, Math.random() * program.usersStartTime * 1000));

        const u = new User(hubUrl, USERS[i][0], USERS[i][1], eventEmitter);
        const userActiveDurationSeconds = parseFloat(program.minUserActiveTime) + (Math.random() * (parseFloat(program.maxUserActiveTime) - parseFloat(program.minUserActiveTime)));
        await u.login();
        await u.startServer();
        await u.startKernel();
        if (program.type == 'rw'){
            await u.executeCodeRW(userActiveDurationSeconds * 1000);
        } else{
            await u.executeCodeCPU(userActiveDurationSeconds * 1000);    
        }
        
        await u.stopKernel();
        await u.stopServer();

    }

    for(let i=0; i < userCount; i++) {
        launch(i);
    }
}

program
    .version('0.1')
    .option('--type [type]', 'stress test cpu usage and memory (cpu) or disk read and write (rw)', 'cpu')
    .option('--min-user-active-time [min-user-active-time]', 'Minimum amount of seconds users should be active', 60)
    .option('--max-user-active-time [max-user-active-time]', 'Maximum amount of seconds users should be active', 600)
    .option('--users-start-time [users-start-time]', 'Period of time (seconds) to distribute starting the users in', 300)
    .option('--user-prefix [user-prefix]', 'Prefix to use for generating usernames', os.hostname())
    .option('--events-tcp-server [events-tcp-server]', 'Address of TCP server that will receive JSON events')
    .arguments('<hub-url> <user-count>')
    .action(main)
    .parse(process.argv);
