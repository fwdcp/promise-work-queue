const assign = require('lodash/assign');
const eq = require('lodash/eq');
const findIndex = require('lodash/findIndex');
const isEmpty = require('lodash/isEmpty');

const EQUAL_TASK_ACTIONS = Object.freeze({
    KEEP_BOTH: Symbol('keepBoth'),
    KEEP_OLD: Symbol('keepOld'),
    KEEP_NEW: Symbol('keepNew')
});

function createDeferred() {
    let deferred = {};

    deferred.promise = new Promise((resolve, reject) => {
        deferred.resolve = resolve;
        deferred.reject = reject;
    });

    return deferred;
}

class PromiseWorkQueue {
    constructor({
        taskComparator = eq,
        equalTaskAction = EQUAL_TASK_ACTIONS.KEEP_BOTH
    } = {}) {
        this.taskComparator = taskComparator;
        this.equalTaskAction = equalTaskAction;

        this.queue = [];
        this.status = {
            stopping: false,
            stopPromise: Promise.resolve(),
            newTaskRequests: []
        };
    }

    async processTask(info) {
        let newTask = {
            info
        };

        if (this.equalTaskAction !== EQUAL_TASK_ACTIONS.KEEP_BOTH) {
            let existingTaskIndex = findIndex(this.queue, (task) => this.taskComparator(task.info, newTask.info));

            if (existingTaskIndex !== -1) {
                if (this.equalTaskAction === EQUAL_TASK_ACTIONS.KEEP_OLD) {
                    let result = await this.queue[existingTaskIndex].promise;
                    return result;
                }
                else if (this.equalTaskAction === EQUAL_TASK_ACTIONS.KEEP_NEW) {
                    let oldTask = this.queue[existingTaskIndex];

                    newTask.promise = oldTask.promise;
                    newTask.resolve = oldTask.resolve;
                    newTask.reject = oldTask.reject;

                    this.queue.splice(existingTaskIndex, 1);
                }
            }
        }

        if (!newTask.promise) {
            let deferred = createDeferred();
            assign(newTask, deferred);
        }

        this.queue.push(newTask);

        if (!isEmpty(this.status.newTaskRequests)) {
            let newTaskRequest = this.status.newTaskRequests.shift();

            newTaskRequest.resolve();
        }

        let result = await newTask.promise;
        return result;
    }

    async run(handler) {
        let status = this.status;

        let runPromise = (async function() {
            while (!status.stopping) {
                try {
                    while (isEmpty(this.queue)) {
                        let newTaskRequest = createDeferred();

                        this.status.newTaskRequests.push(newTaskRequest);

                        await newTaskRequest.promise;
                    }

                    let task = this.queue.shift();

                    try {
                        let result = await handler(task.info);

                        task.resolve(result);
                    }
                    catch (err) {
                        task.reject(err);
                    }
                }
                catch (err) {
                    // stopping or some other external condition, end loop
                    break;
                }
            }
        }).call(this);

        status.stopPromise = Promise.all([status.stopPromise, runPromise]);

        await runPromise;
    }

    async stop() {
        let status = this.status;

        status.stopping = true;
        for (let newTaskRequest of status.newTaskRequests) {
            newTaskRequest.reject('stop requested');
        }

        this.status = {
            stopping: false,
            stopPromise: Promise.resolve(),
            newTaskRequests: []
        };

        await status.stopPromise;
    }
}

PromiseWorkQueue.EQUAL_TASK_ACTIONS = EQUAL_TASK_ACTIONS;

module.exports = PromiseWorkQueue;
