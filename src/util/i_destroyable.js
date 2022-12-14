/**
 * An interface to standardize how objects are destroyed.
 *
 * @interface
 * @exportInterface
 */
 class i_destroyable {
  /**
   * Request that this object be destroyed, releasing all resources and shutting
   * down all operations. Returns a Promise which is resolved when destruction
   * is complete. This Promise should never be rejected.
   *
   * @return {!Promise}
   * @exportInterface
   */
  destroy() {}
};

export default i_destroyable;
