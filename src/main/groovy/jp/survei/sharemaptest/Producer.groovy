/*
 *   Copyright (c) Genetec Corporation. All Rights Reserved.
 */

package jp.survei.sharemaptest

import jp.survei.base.BusMod

import org.vertx.java.core.Future

class Producer extends BusMod {

  def spec = [:]

  def rand = new Random()
  def targets
  def lastID

  def notify(id) { sendEvent('consumer', [action:'notify', id:id]) }

  // timed actions
  static final def items = ['name', 'age', 'set']
  def update() {
    def id = targets[rand.nextInt(targets.size())]
    String accessname = "share.${id}"
    def item = items[rand.nextInt(items.size())]

    logger.info "update(${id}) of ${item}"
    switch(item) {
    case 'name': //
      def map = vertx.sharedData.getMap(accessname)
      map.name += 'Up'
      break
    case 'age': //
      def map = vertx.sharedData.getMap(accessname)
      map.age += 1
      break
    case 'set': //
      def set = vertx.sharedData.getSet(accessname)
      set << 'Up'
      break
    default: //
      logger.warn "Foo? ${item}"
      break
    }
    notify(id)
  }

  def add() {
    def id = ++lastID
    targets << id
    String accessname = "share.${id}"
    def map = vertx.sharedData.getMap(accessname)
    def set = vertx.sharedData.getSet(accessname)
    map.id = id
    map.name = "${id}taro" as String
    map.age = id * 10
    (0..id).each { set << it }
    notify(id)
  }

  def remove() {
    def id = targets[rand.nextInt(targets.size())]
    targets.remove(id)
    String accessname = "share.${id}"
    vertx.sharedData.removeMap(accessname)
    notify(id)
  }

  def show() {
    logger.warn "CURRENT STATUS:"
    targets.each {
      String accessname = "share.${it}"
      logger.warn "${it}: ${vertx.sharedData.getMap(accessname)}  ${vertx.sharedData.getSet(accessname)}"
    }
  }

  // event actions
  def modify(msg, handler) {
    logger.warn "${msg.id} is removed."
    String accessname = "share.${id}"
    def map = vertx.sharedData.getMap(accessname)
    logger.warn "${accessname} = ${map}"
    show()
  }

  @Override def start(Future<Void> sr) {
    super.start()
    logger.info "Start Producer"
    def confresult = chkconfig(config, spec)   // verify configuration.
    logger.debug "chkconfig returns ${confresult}."
    if (confresult) sr.setFailure(confresult) // something wrong.
    else {
      targets = vertx.sharedData.getSet('targets')
      lastID = 0
      while(lastID < 10) add()
      vertx.setPeriodic(10000) { update() }
      vertx.setPeriodic(30000) { add() }
      //      vertx.setPeriodic(45000) { remove() }
      vertx.setPeriodic(60000) { show() }
      listenEventExecAction('producer')
      show()
      sr.setResult(null)
    }
  }

  @Override def stop() {
    logger.info "Producer Stopped."
  }
}
