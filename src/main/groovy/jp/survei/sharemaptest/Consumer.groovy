/*
 *   Copyright (c) Genetec Corporation. All Rights Reserved.
 */

package jp.survei.sharemaptest

import jp.survei.base.BusMod

import org.vertx.java.core.Future

class Consumer extends BusMod {

  def spec = [:]
  def rand = new Random()
  def targets

  def notify(msg, handler) {
    def id = msg.id
    String accessname = "share.${id}"
    if (!targets.contains(id)) logger.fatal "Targets ${id} removed."
    else logger.fatal "Update ${id} ${vertx.sharedData.getMap(accessname)} ${vertx.sharedData.getSet(accessname)}"
  }

  @Override def start(Future<Void> sr) {
    super.start()
    logger.fatal "Start Consumer"
    def confresult = chkconfig(config, spec)   // verify configuration.
    logger.debug "chkconfig returns ${confresult}."
    if (confresult) sr.setFailure(confresult) // something wrong.
    else {
      targets = vertx.sharedData.getSet('targets')
      listenEventExecAction('consumer')
      vertx.setPeriodic(90000) {
        def id = targets[rand.nextInt(targets.size())]
        targets.remove(id)
        String accessname = "share.${id}"
        vertx.sharedData.removeMap(accessname)
        sendEvent('producer', [action:'modify', id:id])
      }
      sr.setResult(null)
    }
  }

  @Override def stop() {
    logger.fatal "Consumer Stopped."
  }
}
