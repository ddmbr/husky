// This is a customizing plugin for Eigen(https://eigen.tuxfamily.org/dox/TopicCustomizing_Plugins.html)
// for allowing dense vector dotting sparse vector (originally only sparse vector dotting dense vector is allowed)
template <typename OtherDerived>
EIGEN_STRONG_INLINE auto dot(const SparseMatrixBase<OtherDerived>& other) const {
    return other.dot(*this);
}
