'use strict';

var _ = require('lodash');

/**
 * This is a generic API free from any proprietery code. TODO: open-source
*/

/**
 * Assigns transport to the client pipeline
*/
function useTransport(transportFactory, config) {
    var handlers = [];

    if (typeof transportFactory === 'string') {
        transportFactory = require(transportFactory);
    }

    var transport = transportFactory(config);

    return {
        use: function use(handlerFactory, config) {
            handlers.push(handlerFactory(config));
            return this;
        },

        create: function create(context) {
            var requestNextHandlers = handlers.slice();
            var requestPrevHandlers = [];
            var responseHandlers = [];

            var requestContext = context ?
                _.clone(context) : {};

            var responseContext = {};
            var contextUse = 0;

            requestContext.use = function use(handlerFactory, config) {
                requestNextHandlers.splice(contextUse++, 0, handlerFactory(config));
                return this;
            };

            responseContext.next = function next(err, response) {
                responseContext.error = err === undefined ? responseHandlers.error : err;
                responseContext.response = response ? response : responseContext.response;
                transportPhase = false;

                var handler = responseHandlers.shift();
                if (!handler) {
                    throw new Error('Make sure requestContext.next or responseContext.next is not called multiple times');
                }
                // adjust position of request handlers
                requestNextHandlers.unshift(requestPrevHandlers.pop());
                handler();
            };

            var transportPhase = false;

            requestContext.next = function next(callback) {
                var handler = requestNextHandlers.shift();
                if (handler) {
                    requestPrevHandlers.push(handler);
                }
                if (!handler && !transportPhase) {
                    handler = transport;
                    transportPhase = true;
                }

                if (!handler && transportPhase) {
                    throw new Error('Make sure responseContext.next is called instead of requestContext.next in transport');
                }

                // add callback
                responseHandlers.unshift(callback || function noop(err) {
                    // when handler does not need a response flow, we simulate one
                    // for the given cycle
                    responseContext.next(err);
                });
                handler(requestContext, responseContext);
            };

            if (transport.api) {
                return transport.api(requestContext, responseContext);
            }

            return function generic(request, callback) {
                requestContext.request = request;
                requestContext.next(function onResponse() {
                    callback(responseContext.error, responseContext.response);
                });
            };
        }
    };
}
module.exports.transport = useTransport;
