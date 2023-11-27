#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "hash_table.h"
#include "neighbour.h"
#include "packet.h"
#include "requests.h"
#include "server.h"
#include "util.h"

// actual underlying hash table
htable **ht = NULL;
rtable **rt = NULL;

// chord peers
peer *self = NULL;
peer *pred = NULL;
peer *succ = NULL;

#define FTABLE_SIZE 5 // number of rows in ftable

// finger table
typedef struct ftable {
  int *ftable_start_node;       // ID of node identifier
  int *ftable_responsible_node; // ID of successor
  int size_of_ftable;
} ftable;

ftable* my_ftable;

/*
 * @brief returns a pointer to an initilized ftable
 * @auth Mostafa
 * @param size = number of rows in ftable
 */
ftable *init_ftable(int size) {
  ftable *new_ftable = (ftable *)malloc(sizeof(ftable));
  new_ftable->ftable_start_node = (int *)calloc(size, sizeof(int));
  new_ftable->ftable_responsible_node = (int *)calloc(size, sizeof(int));
  new_ftable->size_of_ftable = size;
  return new_ftable;
}

/**
 * @brief Forward a packet to a peer.
 *
 * @param peer The peer to forward the request to
 * @param pack The packet to forward
 * @return int The status of the sending procedure
 */
int forward(peer *p, packet *pack) {
    // check whether we can connect to the peer
    if (peer_connect(p) != 0) {
        fprintf(stderr, "Failed to connect to peer %s:%d\n", p->hostname,
                p->port);
        return -1;
    }

    size_t data_len;
    unsigned char *raw = packet_serialize(pack, &data_len);
    int status = sendall(p->socket, raw, data_len);
    free(raw);
    raw = NULL;

    peer_disconnect(p);
    return status;
}

/**
 * send notify message to peer p
 * @param p
 * return sending status
 */
int notify(packet * p, peer * update_peer){

    peer *target_peer = peer_from_packet(p);
    target_peer->node_id = p->node_id;

    // create notify reply
    packet *notify_pack = packet_new();
    notify_pack->flags = PKT_FLAG_NTFY|PKT_FLAG_CTRL;
    notify_pack->node_ip = peer_get_ip(update_peer); // i think this works to get the correct ip address
    notify_pack->node_port = update_peer->port;
    notify_pack->node_id = update_peer->node_id;
    notify_pack->hash_id = update_peer->node_id;

    // check whether we can connect to the peer
    if (peer_connect(target_peer) != 0) {
        fprintf(stderr, "Could not connect to questioner of lookup at %s:%d\n!",
                target_peer->hostname, target_peer->port);
        peer_free(target_peer);
        return CB_REMOVE_CLIENT;
    }

    size_t data_len;
    unsigned char *raw = packet_serialize(notify_pack, &data_len);
    free(notify_pack);
    sendall(target_peer->socket, raw, data_len);
    fprintf(stderr, "  Sent NOTIFY packet to node: %d at %s  \n  ",target_peer->node_id, target_peer->hostname);
    free(raw);
    raw = NULL;
    peer_disconnect(target_peer);
    peer_free(target_peer);
    return CB_REMOVE_CLIENT;
}

/**
 * send join request to known node in existing DHT
 * from us
 * @param node
 *
 */
void init_join_request(char * ip, char * port) {

    // from peer_from_packet method
    peer *known_peer = (peer *)malloc(sizeof(peer));
    memset(known_peer, 0, sizeof(peer));

    known_peer->hostname = ip;
    long int tmp = strtol(port, NULL, 10);
    known_peer->port = tmp;

    packet * join_packet = packet_new();
    join_packet->flags = PKT_FLAG_JOIN | PKT_FLAG_CTRL;
    join_packet->node_ip = peer_get_ip(self); // sth like this to convert to uint32_t ip address
    join_packet->node_port = self->port;
    join_packet->node_id = self->node_id;
    join_packet->hash_id = self->node_id;

    forward(known_peer, join_packet );
    fprintf(stderr, "  Node: %d Sent JOIN packet to : %s  \n  ", self->node_id, known_peer->hostname);
    // free(join_packet)?
    // free(known_peer);
}

/**
 * @brief Forward a request to the successor.
 *
 * @param srv The server
 * @param csocket The scokent of the client
 * @param p The packet to forward
 * @param n The peer to forward to
 * @return int The callback status
 */
int proxy_request(server *srv, int csocket, packet *p, peer *n) {
    // check whether we can connect to the peer
    if (peer_connect(n) != 0) {
        fprintf(stderr,
                "Could not connect to peer %s:%d to proxy request for client!",
                n->hostname, n->port);
        return CB_REMOVE_CLIENT;
    }

    size_t data_len;
    unsigned char *raw = packet_serialize(p, &data_len);
    sendall(n->socket, raw, data_len);
    free(raw);
    raw = NULL;

    size_t rsp_len = 0;
    unsigned char *rsp = recvall(n->socket, &rsp_len);

    // Just pipe everything through unfiltered. Yolo!
    sendall(csocket, rsp, rsp_len);
    free(rsp);

    return CB_REMOVE_CLIENT;
}

/**
 * @brief Lookup the peer responsible for a hash_id.
 *
 * @param hash_id The hash to lookup
 * @return int The callback status
 */
int lookup_peer(uint16_t hash_id) {
    // We could see whether or not we need to repeat the lookup

    // build a new packet for the lookup
    packet *lkp = packet_new();
    lkp->flags = PKT_FLAG_CTRL | PKT_FLAG_LKUP;
    lkp->hash_id = hash_id;
    lkp->node_id = self->node_id;
    lkp->node_port = self->port;

    lkp->node_ip = peer_get_ip(self);

  forward(succ, lkp);
  return 0;
}

int power( int base, int exponent){
    int result = 1;
    for (int i = 0; i < exponent; i++){
        result = result * base;
    }

    return result;
}
/*
 * @brief (re)sets the values of ftable_start_node and ftable_responsible_node
 * @auth: Mostafa
 * @return: 1 if succesful, else 0
 */
int build_ftable(ftable *new_ftable) {
  // TODO do it in a loop
  // now only for self->node_id +1

  // i is index of fable_row

  for (int i = 0; i < new_ftable->size_of_ftable; i++) {
    //double result = pow(double(2), double(i));
    int result = power(2, i);
    uint16_t truncatedResult = (uint16_t)result;
    new_ftable->ftable_start_node[0] = truncatedResult + self->node_id;
    unsigned char buffer[1000];     // buffer for receiving reply packet
    lookup_peer(self->node_id + 1); // lookup the responsible node
    size_t buf_len = recv(self->socket, &buffer, sizeof(buffer),
                          0); // receiving the reply packet as string
    packet *reply_packet;     // preparing pointer for decoding the string
    reply_packet = packet_decode(buffer, buf_len); // decoding the string
    new_ftable->ftable_responsible_node[0] = reply_packet->node_id;
  }
  // TODO check if ftable was built

  return 1; // for now always return 1
}

/**
 * @brief Handle a client request we are resonspible for.
 *
 * @param c The client
 * @param p The packet
 * @return int The callback status
 */
int handle_own_request(server* srv, client *c, packet *p) {
    // build a new packet for the request
    packet *rsp = packet_new();

    if (p->flags & PKT_FLAG_GET) {
        // this is a GET request
        htable *entry = htable_get(ht, p->key, p->key_len);
        if (entry != NULL) {
            rsp->flags = PKT_FLAG_GET | PKT_FLAG_ACK;

            rsp->key = (unsigned char *)malloc(entry->key_len);
            rsp->key_len = entry->key_len;
            memcpy(rsp->key, entry->key, entry->key_len);

            rsp->value = (unsigned char *)malloc(entry->value_len);
            rsp->value_len = entry->value_len;
            memcpy(rsp->value, entry->value, entry->value_len);
        } else {
            rsp->flags = PKT_FLAG_GET;
            rsp->key = (unsigned char *)malloc(p->key_len);
            rsp->key_len = p->key_len;
            memcpy(rsp->key, p->key, p->key_len);
        }
    } else if (p->flags & PKT_FLAG_SET) {
        // this is a SET request
        rsp->flags = PKT_FLAG_SET | PKT_FLAG_ACK;
        htable_set(ht, p->key, p->key_len, p->value, p->value_len);
    } else if (p->flags & PKT_FLAG_DEL) {
        // this is a DELETE request
        int status = htable_delete(ht, p->key, p->key_len);

        if (status == 0) {
            rsp->flags = PKT_FLAG_DEL | PKT_FLAG_ACK;
        } else {
            rsp->flags = PKT_FLAG_DEL;
        }
    } else {
        // send some default data
        rsp->flags = p->flags | PKT_FLAG_ACK;
        rsp->key = (unsigned char *)strdup("Rick Astley");
        rsp->key_len = strlen((char *)rsp->key);
        rsp->value = (unsigned char *)strdup("Never Gonna Give You Up!\n");
        rsp->value_len = strlen((char *)rsp->value);
    }

    size_t data_len;
    unsigned char *raw = packet_serialize(rsp, &data_len);
    free(rsp);
    sendall(c->socket, raw, data_len);
    free(raw);
    raw = NULL;

    return CB_REMOVE_CLIENT;
}

/**
 * @brief Answer a lookup request from a peer.
 *
 * @param p The packet
 * @param n The peer
 * @return int The callback status
 */
int answer_lookup(packet *p, peer *n) {
  peer *questioner = peer_from_packet(p);

  // check whether we can connect to the peer
  if (peer_connect(questioner) != 0) {
    fprintf(stderr, "Could not connect to questioner of lookup at %s:%d\n!",
            questioner->hostname, questioner->port);
    peer_free(questioner);
    return CB_REMOVE_CLIENT;
  }

  // build a new packet for the response
  packet *rsp = packet_new();
  rsp->flags = PKT_FLAG_CTRL | PKT_FLAG_RPLY;
  rsp->hash_id = p->hash_id;
  rsp->node_id = n->node_id;
  rsp->node_port = n->port;
  rsp->node_ip = peer_get_ip(n);

  size_t data_len;
  unsigned char *raw = packet_serialize(rsp, &data_len);
  free(rsp);
  sendall(questioner->socket, raw, data_len);
  free(raw);
  raw = NULL;
  peer_disconnect(questioner);
  peer_free(questioner);
  return CB_REMOVE_CLIENT;
}

/**
 * @brief Answer a ftable request from a peer.
 *
 * @param p The packet from the requester
 * @return int The callback status
 * @auth Mostafa
 */

int answer_fngr(packet *p) {
  peer *questioner = peer_from_packet(p);

  // check whether we can connect to the peer
  if (peer_connect(questioner) != 0) {
    fprintf(stderr, "Could not connect to questioner of lookup at %s:%d\n!",
            questioner->hostname, questioner->port);
    peer_free(questioner);
    return CB_REMOVE_CLIENT;
  }

  // build a new packet for the response
  packet *rsp = packet_new();
  rsp->flags = PKT_FLAG_CTRL | PKT_FLAG_FACK;
  rsp->hash_id = p->hash_id;
  rsp->node_id = self->node_id;
  rsp->node_port = self->port;
  rsp->node_ip = peer_get_ip(self);

  size_t data_len;
  unsigned char *raw = packet_serialize(rsp, &data_len);
  free(rsp);
  sendall(questioner->socket, raw, data_len);
  free(raw);
  raw = NULL;
  peer_disconnect(questioner);
  peer_free(questioner);
  return CB_REMOVE_CLIENT;
}

/**
 * @brief Handle a key request request from a client.
 *
 * @param srv The server
 * @param c The client
 * @param p The packet
 * @return int The callback status
 */
int handle_packet_data(server *srv, client *c, packet *p) {
    // Hash the key of the <key, value> pair to use for the hash table
    uint16_t hash_id = pseudo_hash(p->key, p->key_len);
    fprintf(stderr, "Hash id: %d\n", hash_id);

    // Forward the packet to the correct peer
    if (peer_is_responsible(pred->node_id, self->node_id, hash_id)) {
        // We are responsible for this key
        fprintf(stderr, "We are responsible.\n");
        return handle_own_request(srv, c, p);
    } else if (peer_is_responsible(self->node_id, succ->node_id, hash_id)) {
        // Our successor is responsible for this key
        fprintf(stderr, "Successor's business.\n");
        return proxy_request(srv, c->socket, p, succ);
    } else {
        // We need to find the peer responsible for this key
        fprintf(stderr, "No idea! Just looking it up!.\n");
        add_request(rt, hash_id, c->socket, p);
        lookup_peer(hash_id);
        return CB_OK;
    }
}

/**
 * @brief Handle a control packet from another peer.
 * Lookup vs. Proxy Reply
 *
 * @param srv The server
 * @param c The client
 * @param p The packet
 * @return int The callback status
 */
int handle_packet_ctrl(server *srv, client *c, packet *p) {

    //fprintf(stderr, "Handling control packet...\n");

    if (p->flags & PKT_FLAG_LKUP) {
        fprintf(stderr, "  Received LookUp message  \n");
        // we received a lookup request
        if (peer_is_responsible(pred->node_id, self->node_id, p->hash_id)) {
            // Our business
            fprintf(stderr, "Lol! This should not happen!\n");
            return answer_lookup(p, self);
        } else if (peer_is_responsible(self->node_id, succ->node_id,
                                       p->hash_id)) {
            return answer_lookup(p, succ);
        } else {
            // Great! Somebody else's job!
            forward(succ, p);
        }
    } else if (p->flags & PKT_FLAG_RPLY) {
        // Look for open requests and proxy them
        fprintf(stderr, "  Received LookUp REPLY message  \n");
        peer *n = peer_from_packet(p);
        for (request *r = get_requests(rt, p->hash_id); r != NULL;
             r = r->next) {
            proxy_request(srv, r->socket, r->packet, n);
            server_close_socket(srv, r->socket);
        }
        clear_requests(rt, p->hash_id);

    // handle JOIN messages
    } else if (p->flags & PKT_FLAG_JOIN) {
        fprintf(stderr, "  Received Join message\n  ");
    /**
     *
     * Extend handled control messages.
     * For the first task, this means that join-, stabilize-, and notify-messages should be understood.
     * For the second task, finger- and f-ack-messages need to be used as well.
     **/
    //else if(p->flags & PKT_FLAG_JOIN){


        peer* packet_peer = peer_from_packet(p); // only sets hostname and port!
        packet_peer->node_id = p->node_id;

        // TODO node seems to not feel responsible
        // either wrong check for responsibility or not forwarded to correct node

        if(pred == NULL){
            // there is only one node in ring
            // we are the only node ==> we are responsible
            fprintf(stderr, "  This node is single => it is responsible!  \n");
            pred = packet_peer;
            // send notify pack, with self as new succesor
            return notify(p, self);


        }else if (peer_is_responsible(pred->node_id, self->node_id, packet_peer->node_id) == 1){
            fprintf(stderr, "  This node is responsible!  \n");
            pred = packet_peer;
            // send notify pack, with self as new succesor
            return notify(p, self);


        }else{
            fprintf(stderr, "  This node is not responsible! Forward to node's succesor.  \n");
            // forward
            return forward(succ, p);
        }

    } else if(p->flags & PKT_FLAG_NTFY){
        fprintf(stderr, "  Received Notify message  \n");
            succ = peer_from_packet(p);
            return CB_OK;

    } else if(p->flags & PKT_FLAG_STAB){
        fprintf(stderr, "  Received Stabilize message  \n");
            // update predecessor
            notify(p, pred);
            return CB_OK;



  } else if (p->flags & PKT_FLAG_FNGR) {
    fprintf(stderr, "  Received FingerTable message  \n");
    /* int succ = build_ftable(my_ftable);
     if (succ) {
         // send acknowledgement
     } else {
         fprintf(stderr, "Couldn't build finger table\n");
     }*/

    // send fack back
    answer_fngr(p);
    return CB_OK;
  } else if (p->flags & PKT_FLAG_FACK) {
    return CB_OK;
  }

  // else if finger==1
  // then build_fingertable()
  // forward peer_from_packet, packet with fack=1
  //
  // else if fack==1
  // then return CB_OK
  //}
  return CB_REMOVE_CLIENT;
}

/**
 * @brief Handle a received packet.
 * This can be a key request received from a client or a control packet from
 * another peer.
 *
 * @param srv The server instance
 * @param c The client instance
 * @param p The packet instance
 * @return int The callback status
 */
int handle_packet(server *srv, client *c, packet *p) {
    fprintf(stderr, "  Node: %d at %s  ", self->node_id, self->hostname);
    if (p->flags & PKT_FLAG_CTRL) {
        return handle_packet_ctrl(srv, c, p);
    } else {
        return handle_packet_data(srv, c, p);
    }
}

/**
 * @brief Main entry for a peer of the chord ring.
 *
 * TODO:
 * Modify usage of peer. Accept:
 * 1. Own IP and port;
 * 2. Own ID (optional, zero if not passed);
 * 3. IP and port of Node in existing DHT. This is optional: If not passed, establish new DHT, otherwise join existing.
 *
 * @param argc The number of arguments
 * @param argv The arguments
 * @return int The exit code
 */
int main(int argc, char **argv) {

    // Read arguments for self
    uint16_t idSelf;
    char * portSelf;
    char *hostSelf;
    if(argc > 2){
        // only IP and PORT provided, required
        hostSelf = argv[1];
        portSelf = argv[2];

    }else{
        fprintf(stderr, "Not enough args! Only received: %d arguments. Expected at least 2 for IP and PORT \n", argc);
        portSelf = NULL;
    }

    if(argc > 3){
        // id provided as well, optional
        idSelf = strtoul(argv[3], NULL, 10);

    }else{
        // default id
        idSelf = 0;

    }

    // Initialize all chord peers
    self = peer_init(
            idSelf, hostSelf,
            portSelf); //  Not really necessary but convenient to store us as a peer

    if(argc > 5){ // or > 4
        // node to join provided, optional
        init_join_request(argv[4], argv[5]);

    }// else we are the first node in a new DHT



    // Initialize outer server for communication with clients
    server *srv = server_setup(portSelf);
    if (srv == NULL) {
        fprintf(stderr, "Server setup failed!\n");
        return -1;
    }
    // Initialize hash table
    ht = (htable **)malloc(sizeof(htable *));
    // Initiale reuqest table
    rt = (rtable **)malloc(sizeof(rtable *));
    *ht = NULL;
    *rt = NULL;

    srv->packet_cb = handle_packet;
    server_run(srv);
    close(srv->socket);
}



// sudo rnvs-tb-dht -s .
